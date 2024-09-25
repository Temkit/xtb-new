require('dotenv').config();

const XAPI = require("xapi-node").default;
const { CMD_FIELD, PERIOD_FIELD } = require("xapi-node");
const { SMA, VWAP, RSI, ATR } = require("technicalindicators");
const AWS = require("aws-sdk");
const { v4: uuidv4 } = require("uuid");
const winston = require('winston');

// Constants
const MAX_CONNECTIONS = 50;
const PERIOD = PERIOD_FIELD.PERIOD_M15;
const RISK = 0.15;
const RRR = 3;
const PROFIT_THRESHOLD = 100;
const LOSS_THRESHOLD = 150; // Added loss threshold constant
const UPDATE_INTERVAL = 900000; // 15 minutes
const RETRY_ATTEMPTS = 3;
const RETRY_DELAY = 1000;
const MULTIPLIER = 4;

const symbols = [
  "EURAUD", "AUDCHF", "NZDUSD", "USDSGD", "USDCHF", "GBPUSD", "EURCHF", "EURCAD",
  "USDJPY", "EURGBP", "AUDCAD", "USDCAD", "AUDNZD", "NZDCHF", "GBPAUD", "GBPCHF",
  "AUDUSD", "EURUSD", "CADCHF", "GOLD", "OIL.WTI"
];

// AWS Configuration
AWS.config.update({
  region: process.env.AWS_REGION,
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
});

// Logger Configuration
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: 'error.log', level: 'error' }),
    new winston.transports.File({ filename: 'combined.log' })
  ]
});

// Global variables
const connectionPool = [];
const dynamoDB = new AWS.DynamoDB.DocumentClient();
const TRADES_TABLE = "trades";
const symbolCache = new Map();
const chartDataCache = new Map();
let positionsClosedDueToProfit = false; // Added flag to track if positions were closed due to profit threshold

// Utility Functions
async function withRetry(fn, retries = RETRY_ATTEMPTS, delay = RETRY_DELAY) {
  try {
    return await fn();
  } catch (error) {
    if (retries > 0) {
      logger.warn(`Retrying operation. Attempts left: ${retries}`);
      await new Promise((res) => setTimeout(res, delay));
      return withRetry(fn, retries - 1, delay * 2);
    } else {
      throw error;
    }
  }
}

async function createConnection() {
  const x = new XAPI({
    accountId: process.env.XTB_ACCOUNT_ID,
    password: process.env.XTB_PASSWORD,
    host: "ws.xtb.com",
    type: "demo",
  });

  await x.connect();

  x.onTransactionUpdate(({ key, trade }) => {
    logger.info("Transaction Update", { key, trade });
  });

  return x;
}

async function initializeConnectionPool() {
  const connections = await Promise.all(
    Array.from({ length: MAX_CONNECTIONS }, () => withRetry(() => createConnection()))
  );
  connectionPool.push(...connections);
  logger.info(`${MAX_CONNECTIONS} connections are ready`);
}

async function fetchAndCacheSymbolData() {
  const x = connectionPool[0];
  for (const symbol of symbols) {
    const { data } = await withRetry(() => x.Socket.send.getSymbol(symbol));
    symbolCache.set(symbol, data.returnData);
  }
  logger.info("Symbol data cached for all symbols");
}

async function fetchAndCacheChartData() {
  const x = connectionPool[0];
  const startTime = getYesterdayMidnightTimestamp(15);

  for (const symbol of symbols) {
    const response = await withRetry(() => x.Socket.send.getChartLastRequest(PERIOD, startTime, symbol));
    const allPrices = extractPrices(response);
    chartDataCache.set(symbol, allPrices);
  }
  logger.info("Chart data cached for all symbols");
}

async function processSymbol(symbol) {
  const x = connectionPool[Math.floor(Math.random() * connectionPool.length)];

  try {
    const returnData = symbolCache.get(symbol);
    if (!returnData) {
      logger.error(`No cached data for symbol ${symbol}`);
      return;
    }

    const accountInfo = await withRetry(() => x.Socket.send.getMarginLevel());

    const accountBalance = accountInfo.data.returnData.balance;
    const equityBalance = accountInfo.data.returnData.equity;

    const profit = equityBalance - accountBalance;
    logger.info(`Current profit for ${symbol}`, { profit });

    if (profit > PROFIT_THRESHOLD) {
      logger.info(`Profit exceeded threshold for ${symbol}`, { profit, threshold: PROFIT_THRESHOLD });
      const { data: openedTrades } = await withRetry(() => x.Socket.send.getTrades(true));
      await closeAllPositions(x, openedTrades.returnData);
      positionsClosedDueToProfit = true; // Set the flag
      return;
    }

    const allPrices = chartDataCache.get(symbol);
    if (!allPrices) {
      logger.error(`No cached chart data for symbol ${symbol}`);
      return;
    }

    // Derive data for different timeframes
    const pricesVWAP = slicePrices(allPrices, 1);
    const prices5daysMA = allPrices;
    const pricesRSI120 = aggregatePrices(allPrices, 8);
    const pricesRSI960 = aggregatePrices(allPrices, 64);

    const sma = SMA.calculate({ period: 480, values: prices5daysMA.close });
    const vwap = VWAP.calculate({
      high: pricesVWAP.high,
      low: pricesVWAP.low,
      close: pricesVWAP.close,
      volume: pricesVWAP.volume,
    });

    const rsi15 = await calculateRSI(prices5daysMA.close, 14);
    const rsi120 = await calculateRSI(pricesRSI120.close, 14);
    const rsi960 = await calculateRSI(pricesRSI960.close, 14);

    const atr = await calculateATR(prices5daysMA.high, prices5daysMA.low, prices5daysMA.close, 14);

    const lastPrice = prices5daysMA.close[prices5daysMA.close.length - 1];
    logger.info(`${symbol} - Market Data`, {
      lastPrice,
      SMA: sma[sma.length - 1],
      VWAP: vwap[vwap.length - 1],
      RSI15: rsi15[rsi15.length - 1],
      RSI120: rsi120[rsi120.length - 1],
      RSI960: rsi960[rsi960.length - 1],
      ATR: atr[atr.length - 1]
    });

    let tendance = null;
    if (lastPrice > sma[sma.length - 1] && lastPrice > vwap[vwap.length - 1] && rsi15[rsi15.length - 1] < 80 && rsi120[rsi120.length - 1] < 80 && rsi960[rsi960.length - 1] < 80) {
      tendance = CMD_FIELD.BUY;
    } else if (lastPrice < sma[sma.length - 1] && lastPrice < vwap[vwap.length - 1] && rsi15[rsi15.length - 1] > 20 && rsi120[rsi120.length - 1] > 20 && rsi960[rsi960.length - 1] > 20) {
      tendance = CMD_FIELD.SELL;
    }

    const { data: openedTrades } = await withRetry(() => x.Socket.send.getTrades(true));
    const alreadyOpenedSD = openedTrades.returnData.some(
      (trade) => trade.symbol === symbol && trade.cmd === tendance
    );

    const oppositeTendance = tendance === CMD_FIELD.BUY ? CMD_FIELD.SELL : CMD_FIELD.BUY;
    const oppositeTrades = openedTrades.returnData.filter(
      (trade) => trade.symbol === symbol && trade.cmd === oppositeTendance
    );

    if (tendance !== null && !alreadyOpenedSD) {
      logger.info(`Opening new position for ${symbol}`, { tendance });
      const openTradeData = await openPosition(x, symbol, tendance, returnData);
      await storeTrade({
        ...openTradeData,
        action: "OPEN",
        vwap: vwap[vwap.length - 1],
        sma: sma[sma.length - 1],
        rsi15: rsi15[rsi15.length - 1],
        rsi120: rsi120[rsi120.length - 1],
        rsi960: rsi960[rsi960.length - 1],
        atr: atr[atr.length - 1]
      });
    }

    if (oppositeTrades.length > 0) {
      // Calculate total loss of opposite trades
      const totalLoss = oppositeTrades.reduce((acc, trade) => acc + trade.profit, 0);
      if (totalLoss < -LOSS_THRESHOLD) {
        logger.info(`Total loss of opposite positions for ${symbol} exceeds threshold`, { totalLoss, threshold: LOSS_THRESHOLD });
        logger.info(`Closing opposite positions for ${symbol}`, { count: oppositeTrades.length });
        const closedTradesData = await closePositions(x, oppositeTrades);
        for (const closedTrade of closedTradesData) {
          await storeTrade({
            ...closedTrade,
            action: "CLOSE",
            vwap: vwap[vwap.length - 1],
            sma: sma[sma.length - 1],
            rsi15: rsi15[rsi15.length - 1],
            rsi120: rsi120[rsi120.length - 1],
            rsi960: rsi960[rsi960.length - 1],
            atr: atr[atr.length - 1]
          });
        }
      }
    }

  } catch (error) {
    logger.error(`Error processing symbol ${symbol}:`, error);
    if (error.error && error.error.errorDescr) {
      logger.error(`API Error for ${symbol}`, { description: error.error.errorDescr, code: error.error.errorCode });
    }
  }
}

async function calculateRSI(prices, period) {
  return RSI.calculate({
    values: prices,
    period: period
  });
}

async function calculateATR(high, low, close, period) {
  return ATR.calculate({
    high: high,
    low: low,
    close: close,
    period: period
  });
}

async function storeTrade(tradeData) {
  const item = {
    id: uuidv4(),
    timestamp: Date.now(),
    ...tradeData
  };

  const params = {
    TableName: TRADES_TABLE,
    Item: item
  };

  try {
    await withRetry(() => dynamoDB.put(params).promise());
    logger.info("Trade stored in DynamoDB", { tradeId: item.id });
  } catch (error) {
    logger.error("Error storing trade in DynamoDB", { error, item });
  }
}

function extractPrices(response) {
  const prices = {
    open: [],
    high: [],
    low: [],
    close: [],
    volume: [],
  };

  response.data.returnData.rateInfos.forEach((rate) => {
    prices.open.push(rate.open);
    prices.high.push(rate.open + rate.high);
    prices.low.push(rate.open + rate.low);
    prices.close.push(rate.open + rate.close);
    prices.volume.push(rate.vol);
  });

  return prices;
}

function slicePrices(prices, days) {
  const slicePoint = prices.open.length - (days * 24 * 4);
  return {
    open: prices.open.slice(slicePoint),
    high: prices.high.slice(slicePoint),
    low: prices.low.slice(slicePoint),
    close: prices.close.slice(slicePoint),
    volume: prices.volume.slice(slicePoint),
  };
}

function aggregatePrices(prices, interval) {
  const aggregated = {
    open: [],
    high: [],
    low: [],
    close: [],
    volume: [],
  };

  for (let i = 0; i < prices.open.length; i += interval) {
    const slice = {
      open: prices.open.slice(i, i + interval),
      high: prices.high.slice(i, i + interval),
      low: prices.low.slice(i, i + interval),
      close: prices.close.slice(i, i + interval),
      volume: prices.volume.slice(i, i + interval),
    };

    aggregated.open.push(slice.open[0]);
    aggregated.high.push(Math.max(...slice.high));
    aggregated.low.push(Math.min(...slice.low));
    aggregated.close.push(slice.close[slice.close.length - 1]);
    aggregated.volume.push(slice.volume.reduce((a, b) => a + b, 0));
  }

  return aggregated;
}

async function closePositions(x, trades) {
  const closedTrades = [];
  for (const trade of trades) {
    const closeOrder = {
      order: trade.order,
      price: trade.close_price,
      type: 2,
      symbol: trade.symbol,
      volume: trade.volume
    };

    logger.info("Closing position", closeOrder);

    try {
      const result = await withRetry(() => x.Socket.send.tradeTransaction(closeOrder));
      logger.info(`Position closed`, { symbol: trade.symbol, position: trade.position });
      closedTrades.push({
        symbol: trade.symbol,
        position: trade.position,
        closePrice: trade.close_price,
        open_price: trade.open_price,
        volume: trade.volume,
        order: trade.order,
        profit: trade.profit,
        timestamp: trade.timestamp
      });
    } catch (e) {
      logger.error(`Error closing position`, { symbol: trade.symbol, position: trade.position, error: e });
    }
  }
  return closedTrades;
}

async function closeAllPositions(x, trades) {
  for (const trade of trades) {
    const closeOrder = {
      order: trade.order,
      price: trade.close_price,
      type: 2,
      symbol: trade.symbol,
      position: trade.position,
      closePrice: trade.close_price,
      open_price: trade.open_price,
      volume: trade.volume,
      profit: trade.profit,
      timestamp: trade.timestamp
    };

    logger.info("Closing all positions", closeOrder);

    try {
      const result = await withRetry(() => x.Socket.send.tradeTransaction(closeOrder));
      await storeTrade({
        ...closeOrder,
        action: "CLOSE"
      });
      logger.info(`Position closed`, { symbol: trade.symbol, position: trade.position });
    } catch (e) {
      logger.error(`Error closing position`, { symbol: trade.symbol, position: trade.position, error: e });
    }
  }
}

async function openPosition(x, symbol, tendance, returnData) {
  const { ask, bid, lotMin, precision, pipsPrecision } = returnData;
  const price = tendance === CMD_FIELD.BUY ? ask : bid;
  const accountInfo = await withRetry(() => x.Socket.send.getMarginLevel());
  const accountBalance = accountInfo.data.returnData.balance;
  const riskAmount = accountBalance * RISK;
  const pipValue = 1 / Math.pow(10, pipsPrecision);
  const slPips = riskAmount / (lotMin * pipValue);
  const tpPips = slPips * RRR;
  const SL =
    tendance === CMD_FIELD.BUY
      ? price - slPips * pipValue
      : price + slPips * pipValue;
  const TP =
    tendance === CMD_FIELD.BUY
      ? price + tpPips * pipValue
      : price - tpPips * pipValue;

  const order = {
    price,
    symbol,
    cmd: tendance,
    type: 0,
    volume: lotMin * MULTIPLIER
  };

  logger.info("Opening position", order);

  try {
    const { data: opened } = await withRetry(() => x.Socket.send.tradeTransaction(order));
    logger.info(`Position opened`, { symbol, order: opened.returnData.order });
    return {
      symbol,
      tendance,
      openPrice: price,
      volume: lotMin * MULTIPLIER,
      order: opened.returnData.order
    };
  } catch (e) {
    logger.error(`Error opening position`, { symbol, error: e });
    return null;
  }
}

const round = (value, precision) =>
  Math.round(value * Math.pow(10, precision)) / Math.pow(10, precision);

const getYesterdayMidnightTimestamp = (days) => {
  const now = new Date();
  const yesterday = new Date(
    now.getFullYear(),
    now.getMonth(),
    now.getDate() - days
  );
  return yesterday.getTime();
};

async function processBatch(batch) {
  return Promise.all(batch.map((symbol) => processSymbol(symbol)));
}

async function main() {
  try {
    await initializeConnectionPool();

    while (true) {
      logger.info("Starting new trading cycle");

      // Update caches at the start of each main loop
      await fetchAndCacheSymbolData();
      await fetchAndCacheChartData();

      for (let i = 0; i < symbols.length; i += MAX_CONNECTIONS) {
        const batch = symbols.slice(i, i + MAX_CONNECTIONS);
        await processBatch(batch);

        if (positionsClosedDueToProfit) {
          // Reset flag
          positionsClosedDueToProfit = false;
          // Break out of the for loop to restart trading cycle immediately
          logger.info(`Positions closed due to profit threshold. Restarting trading cycle immediately.`);
          break;
        }
      }

      logger.info(`Trading cycle completed. Waiting for ${UPDATE_INTERVAL / 60000} minutes before next cycle.`);
      // Wait for the next iteration
      await new Promise((resolve) => setTimeout(resolve, UPDATE_INTERVAL));
    }
  } catch (error) {
    logger.error("Fatal error in main loop:", error);
    process.exit(1);
  }
}

// Error handling for unhandled promises
process.on('unhandledRejection', (reason, promise) => {
  logger.error('Unhandled Rejection at:', promise, 'reason:', reason);
  // Application specific logging, throwing an error, or other logic here
});

main().catch((error) => {
  logger.error("Fatal error:", error);
  process.exit(1);
});
