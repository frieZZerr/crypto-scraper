const { Storage } = require('@google-cloud/storage');
const { LoggingWinston } = require('@google-cloud/logging-winston');
const winston = require('winston');
const puppeteer = require('puppeteer');

// Initialize the Google Cloud Storage client
const storage = new Storage();
const bucketName = 'crypto-data-storage-bucket';
const bucket = storage.bucket(bucketName);

// Initialize the Google Logging client
const loggingWinston = new LoggingWinston();

// Create a Winston logger that streams to Cloud Logging
// Logs will be written to: "projects/YOUR_PROJECT_ID/logs/winston_log"
let logger;
try {
  logger = winston.createLogger({
    level: 'info',
    transports: [
      new winston.transports.Console(),
      // Add Cloud Logging
      loggingWinston,
    ],
  });
  logger.info('WinstonLogger has been initialized succesfully!');
} catch(e) {
  logger.error('There was an error when initializing WinstonLogger: ', e);
}

/// Generates current timestamp.
///     - Timestamp is attached to generated file name
///         for more readable data storage.
///     - Return Format: -YYYY_MM_DD-HH_MM-PART
function generateTimestamp() {
  const date = new Date(Date.now());
  const part = Math.trunc((date.getHours()+2)/6);
  const dateFormat = date.toISOString().split('T')[0]+"-"+part;
  return dateFormat;
}

async function saveToStorage(data) {
  // Save the scraped data to Google Cloud Storage
  const timestamp = generateTimestamp();

  for(let i = 0; i < data[0].length; i++) {
    let filename = data[0].at(i)+'.json';
    let file = bucket.file(filename);
    let coinData = {
      timestamp: [timestamp],
      price: [data[1].at(i)]
    };

    let [exists] = await file.exists();
    if(exists) {
      let [existingData] = await file.download();
      let buffer = Buffer.from(existingData);
      coinData = JSON.parse(buffer.toString());
      coinData.timestamp.push(timestamp);
      coinData.price.push(data[1].at(i));
    }

    try {
      file.save(JSON.stringify(coinData));
    } catch(err) {
      logger.error('There was an error while trying to save'+filename+' to '+bucketName+'!', err);
    }
  }
}

async function scrapeWebsite(index) {
  let browser;
  try {
    // Launch Puppeteer using a compatible version of Chromium
    browser = await puppeteer.launch({
      headless: 'new'
    });

    // Scrape data from the website using Puppeteer
    const page = await browser.newPage();
    var url = 'https://www.cryptocompare.com/coins/list/all/USD/'+index;
    logger.info('Scraping '+url+'...');
    await page.goto(url);
    page.setDefaultNavigationTimeout(2 * 60 * 1000);

    // Wait for the table to load
    await page.waitForSelector('tbody');

    // Scrape the data from the website and return it as a string in CSV format
    let coinName = await page.$$eval('.mobile-name', el => {
        return el.map(name => name.innerHTML.trim());
    });
    let price = await page.$$eval('.current-price-value', el => {
        return el.map(_price => _price.innerHTML.trim().replace(/\s+/g, '').replace('$', '').replace(',', ''));
    });

    return [ Array.from(coinName), Array.from(price) ];
  } catch(e) {
    logger.error('There was an error while scraping '+url+'. ', e);
  }
  finally {
    await browser?.close();
  }
}

exports.run = async () => {
  for(let i = 1; i <= 5; i++) {
    var chunk = await scrapeWebsite(i);
    try {
      await saveToStorage(chunk);
    } catch(err) {
      logger.error('There was an error while trying to save '+i+'. page. Saved records: '+chunk[0].length);
    }
  }
};
