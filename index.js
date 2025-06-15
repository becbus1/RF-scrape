// ✅ Real Estate Scraper with Stealth & Supabase Integration
const { chromium } = require('playwright-extra');
const stealth = require('puppeteer-extra-plugin-stealth')();
const { createClient } = require('@supabase/supabase-js');

// ✅ Apply stealth plugin to playwright
chromium.use(stealth);

// ✅ Supabase credentials
const supabaseUrl = process.env.SUPABASE_URL || 'https://rskcssgjpbshagjocdre.supabase.co';
const supabaseKey = process.env.SUPABASE_KEY || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJza2Nzc2dqcGJzaGFnam9jZHJlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDk5MzU3ODUsImV4cCI6MjA2NTUxMTc4NX0.dgRp-eXWdIPXsL5R_lCCNfbhjOwM8KpYXixC4sHBf30';
const supabase = createClient(supabaseUrl, supabaseKey);

// ✅ Hardcoded $/sqft benchmarks for zip codes
const zipBenchmarks = {
  "11201": 850,
  "11206": 680,
  "11211": 740,
  "11221": 600,
  "11233": 560,
  "11215": 900,
  "11238": 800,
  "11216": 720,
  "11213": 620,
  "11207": 500
};

const NYC_ZIPS = Object.keys(zipBenchmarks);

// ✅ Random user agents for rotation
const userAgents = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0'
];

async function randomDelay(min = 1000, max = 3000) {
  const delay = Math.floor(Math.random() * (max - min + 1)) + min;
  await new Promise(resolve => setTimeout(resolve, delay));
}

async function setupStealthPage(page) {
  // ✅ Additional stealth measures
  await page.setViewportSize({ width: 1366, height: 768 });
  
  // ✅ Random user agent
  const randomUA = userAgents[Math.floor(Math.random() * userAgents.length)];
  await page.setUserAgent(randomUA);
  
  // ✅ Set realistic headers
  await page.setExtraHTTPHeaders({
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Cache-Control': 'max-age=0'
  });

  // ✅ Override webdriver detection
  await page.addInitScript(() => {
    Object.defineProperty(navigator, 'webdriver', {
      get: () => undefined,
    });
    
    // Remove playwright traces
    delete window.__playwright;
    delete window.__pw_manual;
    delete window.__PW_inspect;
  });
}

async function scrapeRealtorCom(page, zip) {
  const url = `https://www.realtor.com/realestateandhomes-search/${zip}`;
  console.log(`Scraping realtor.com for zip: ${zip}`);
  
  try {
    await page.goto(url, { 
      timeout: 60000,
      waitUntil: 'domcontentloaded'
    });

    // ✅ Wait for listings to load
    await page.waitForSelector('[data-testid="property-card"]', { 
      timeout: 30000 
    }).catch(() => {
      console.log(`No realtor.com listings found for zip ${zip}`);
      return [];
    });

    await randomDelay(2000, 4000);

    const listings = await page.$$('[data-testid="property-card"]');
    console.log(`Found ${listings.length} realtor.com listings for zip ${zip}`);

    const data = [];
    for (const el of listings) {
      try {
        const address = await el.$eval('[data-label="pc-address"]', node => node.textContent.trim()).catch(() => '');
        const priceText = await el.$eval('[data-label="pc-price"]', node => node.textContent.trim()).catch(() => '');
        const sqftText = await el.$eval('[data-label="pc-sqft"]', node => node.textContent.trim()).catch(() => '');

        if (!address || !priceText || !sqftText) continue;

        const price = parseInt(priceText.replace(/[^\d]/g, ''));
        const sqft = parseInt(sqftText.replace(/[^\d]/g, ''));

        if (!price || !sqft || price < 10000 || sqft < 100) continue;

        const pricePerSqft = Math.round(price / sqft);
        const benchmark = zipBenchmarks[zip];
        const score = Math.round(((benchmark - pricePerSqft) / benchmark) * 100);

        data.push({
          address,
          zip,
          price,
          sqft,
          price_per_sqft: pricePerSqft,
          below_market_score: score,
          source: 'realtor.com',
          created_at: new Date().toISOString()
        });

      } catch (listingError) {
        console.error(`Error processing realtor.com listing:`, listingError.message);
      }
    }

    return data;
  } catch (error) {
    console.error(`Failed to scrape realtor.com for zip ${zip}:`, error.message);
    return [];
  }
}

async function scrapeRedfin(page, zip) {
  const url = `https://www.redfin.com/zipcode/${zip}`;
  console.log(`Scraping Redfin for zip: ${zip}`);
  
  try {
    await page.goto(url, { 
      timeout: 60000,
      waitUntil: 'domcontentloaded'
    });

    await page.waitForSelector('[data-rf-test-id="abp-item"]', { 
      timeout: 30000 
    }).catch(() => {
      console.log(`No Redfin listings found for zip ${zip}`);
      return [];
    });

    await randomDelay(2000, 4000);

    const listings = await page.$$('[data-rf-test-id="abp-item"]');
    console.log(`Found ${listings.length} Redfin listings for zip ${zip}`);

    const data = [];
    for (const el of listings) {
      try {
        const address = await el.$eval('[data-rf-test-id="abp-address"]', node => node.textContent.trim()).catch(() => '');
        const priceText = await el.$eval('[data-rf-test-id="abp-price"]', node => node.textContent.trim()).catch(() => '');
        const sqftText = await el.$eval('.stats', node => {
          const text = node.textContent;
          const match = text.match(/([\d,]+)\s+sqft/i);
          return match ? match[1] : '';
        }).catch(() => '');

        if (!address || !priceText || !sqftText) continue;

        const price = parseInt(priceText.replace(/[^\d]/g, ''));
        const sqft = parseInt(sqftText.replace(/,/g, ''));

        if (!price || !sqft || price < 10000 || sqft < 100) continue;

        const pricePerSqft = Math.round(price / sqft);
        const benchmark = zipBenchmarks[zip];
        const score = Math.round(((benchmark - pricePerSqft) / benchmark) * 100);

        data.push({
          address,
          zip,
          price,
          sqft,
          price_per_sqft: pricePerSqft,
          below_market_score: score,
          source: 'redfin.com',
          created_at: new Date().toISOString()
        });

      } catch (listingError) {
        console.error(`Error processing Redfin listing:`, listingError.message);
      }
    }

    return data;
  } catch (error) {
    console.error(`Failed to scrape Redfin for zip ${zip}:`, error.message);
    return [];
  }
}

async function runScraper() {
  let browser;
  
  try {
    console.log('🚀 Starting Real Estate Scraper...');
    
    // ✅ Railway-compatible browser launch with stealth
    browser = await chromium.launch({
      headless: true,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-accelerated-2d-canvas',
        '--no-first-run',
        '--no-zygote',
        '--single-process',
        '--disable-gpu',
        '--disable-web-security',
        '--disable-features=VizDisplayCompositor',
        '--disable-blink-features=AutomationControlled'
      ]
    });

    const page = await browser.newPage();
    await setupStealthPage(page);
    
    let allData = [];

    for (const zip of NYC_ZIPS) {
      console.log(`\n--- Processing zip code: ${zip} ---`);
      
      // ✅ Try realtor.com first, fallback to Redfin
      let zipData = await scrapeRealtorCom(page, zip);
      
      if (zipData.length === 0) {
        console.log(`No realtor.com data for ${zip}, trying Redfin...`);
        zipData = await scrapeRedfin(page, zip);
      }
      
      allData = allData.concat(zipData);
      console.log(`Total listings for ${zip}: ${zipData.length}`);
      
      // ✅ Random delay between zip codes
      await randomDelay(3000, 6000);
    }

    console.log(`\n✅ Scraping complete! Found ${allData.length} total listings`);

    // ✅ Insert data to Supabase
    if (allData.length > 0) {
      // ✅ Insert in batches to avoid timeout
      const batchSize = 100;
      for (let i = 0; i < allData.length; i += batchSize) {
        const batch = allData.slice(i, i + batchSize);
        const { error } = await supabase.from('listings').insert(batch);
        
        if (error) {
          console.error(`Supabase insert error for batch ${i}:`, error.message);
        } else {
          console.log(`✅ Uploaded batch ${Math.floor(i/batchSize) + 1} (${batch.length} listings)`);
        }
      }
      
      console.log('🎉 All data uploaded to Supabase!');
    } else {
      console.log('❌ No valid listings found to upload');
    }

  } catch (error) {
    console.error('❌ Scraper error:', error.message);
    console.error(error.stack);
  } finally {
    if (browser) {
      await browser.close();
    }
  }
}

// ✅ Handle process termination
process.on('SIGTERM', async () => {
  console.log('Received SIGTERM, shutting down gracefully');
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('Received SIGINT, shutting down gracefully');
  process.exit(0);
});

// ✅ Handle uncaught exceptions
process.on('uncaughtException', (error) => {
  console.error('Uncaught Exception:', error);
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
  process.exit(1);
});

console.log('🏠 Real Estate Scraper Starting...');
runScraper().catch(console.error);
