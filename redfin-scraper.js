const axios = require('axios');
const fs = require('fs').promises;

class RedfinAPIScraper {
    constructor() {
        // Enhanced HTTP client with better stealth headers
        this.client = axios.create({
            headers: {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-US,en;q=0.9',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
                'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"macOS"',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-User': '?1',
                'Upgrade-Insecure-Requests': '1',
                'Referer': 'https://www.redfin.com/',
                'Origin': 'https://www.redfin.com',
                'DNT': '1',
                'Connection': 'keep-alive'
            },
            timeout: 30000,
            maxRedirects: 5,
            // Add retry logic for failed requests
            validateStatus: function (status) {
                return status < 500; // Resolve only if status < 500
            }
        });

        // Common API base URLs discovered from network analysis
        this.API_BASE = 'https://www.redfin.com/stingray/api';
        
        // More conservative rate limiting
        this.rateLimitDelay = 3000; // 3 seconds between requests
        this.requestCount = 0;
        this.sessionStartTime = Date.now();

        // NYC-specific region mappings (cached to reduce API calls)
        this.NYC_REGIONS = {
            'Manhattan, NY': { id: 16904, type: 6, name: 'Manhattan' },
            'Brooklyn, NY': { id: 17072, type: 6, name: 'Brooklyn' },
            'Queens, NY': { id: 17085, type: 6, name: 'Queens' },
            'Bronx, NY': { id: 17070, type: 6, name: 'Bronx' },
            'Staten Island, NY': { id: 17112, type: 6, name: 'Staten Island' },
            'New York, NY': { id: 30749, type: 6, name: 'New York City' }
        };

        // Add session cookies simulation
        this.cookies = new Map();
    }

    async delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    /**
     * Enhanced request method with retry logic and better error handling
     */
    async makeRequest(url, params = {}, retries = 3) {
        for (let attempt = 1; attempt <= retries; attempt++) {
            try {
                // Increment request counter for session simulation
                this.requestCount++;
                
                // Add some randomness to delay to appear more human-like
                const randomDelay = this.rateLimitDelay + Math.random() * 1000;
                await this.delay(randomDelay);

                // Update headers for each request to simulate real browsing
                const dynamicHeaders = {
                    ...this.client.defaults.headers,
                    'X-Requested-With': 'XMLHttpRequest',
                    'X-Request-Count': this.requestCount.toString(),
                    'Cache-Control': Math.random() > 0.5 ? 'no-cache' : 'max-age=0'
                };

                console.log(`🌐 Making request (attempt ${attempt}/${retries}): ${url}`);
                
                const response = await this.client.get(url, { 
                    params,
                    headers: dynamicHeaders
                });

                // Store any cookies for session simulation
                if (response.headers['set-cookie']) {
                    response.headers['set-cookie'].forEach(cookie => {
                        const [name, value] = cookie.split('=');
                        this.cookies.set(name, value);
                    });
                }

                console.log(`✅ Request successful (${response.status})`);
                return response;

            } catch (error) {
                console.log(`⚠️ Request failed (attempt ${attempt}/${retries}): ${error.response?.status || error.message}`);
                
                if (error.response?.status === 403 && attempt < retries) {
                    // 403 Forbidden - wait longer before retry
                    console.log(`🔄 Got 403, waiting ${attempt * 5} seconds before retry...`);
                    await this.delay(attempt * 5000);
                } else if (error.response?.status === 429 && attempt < retries) {
                    // 429 Rate Limited - exponential backoff
                    const backoffTime = Math.pow(2, attempt) * 1000;
                    console.log(`⏰ Rate limited, waiting ${backoffTime}ms before retry...`);
                    await this.delay(backoffTime);
                } else if (attempt === retries) {
                    throw error;
                } else {
                    // Other errors - short delay
                    await this.delay(1000);
                }
            }
        }
    }

    /**
     * Get region ID for NYC locations with cached mappings and fallback
     */
    async getRegionId(location) {
        try {
            // Check if it's a known NYC region first (avoids API call)
            if (this.NYC_REGIONS[location]) {
                console.log(`✅ Using cached NYC region: ${this.NYC_REGIONS[location].name}`);
                return this.NYC_REGIONS[location];
            }

            // Fall back to API lookup for other NYC areas
            const url = `${this.API_BASE}/location/search`;
            const params = {
                location: location,
                v: 2
            };
            
            console.log(`🔍 Searching for region ID for: ${location}`);
            const response = await this.makeRequest(url, params);
            
            // Redfin returns data with a safety prefix, remove it
            let jsonData = response.data;
            if (typeof jsonData === 'string') {
                jsonData = jsonData.replace(/^\)\]\}',?\n?/, '');
                jsonData = JSON.parse(jsonData);
            }
            
            if (jsonData.payload && jsonData.payload.exactMatch) {
                const region = jsonData.payload.exactMatch;
                console.log(`✅ Found region: ${region.name} (ID: ${region.id}, Type: ${region.type})`);
                
                // Cache the result for future use
                this.NYC_REGIONS[location] = {
                    id: region.id,
                    type: region.type,
                    name: region.name
                };
                
                return this.NYC_REGIONS[location];
            }
            
            throw new Error('No exact match found for location');
        } catch (error) {
            console.error(`❌ Error getting region ID for ${location}:`, error.message);
            throw error;
        }
    }

    /**
     * Enhanced CSV scraping with better error handling
     */
    async scrapeListingsCSV(regionId, regionType, options = {}) {
        try {
            const url = `${this.API_BASE}/gis-csv`;
            
            // NYC-optimized parameters with better defaults
            const params = {
                al: 1, // Include all listings
                market: 'newyork', // NYC market
                num_homes: Math.min(options.limit || 100, 350), // Respect Redfin's limits
                ord: options.sortBy || 'days-on-redfin-asc',
                page_number: options.page || 1,
                region_id: regionId,
                region_type: regionType,
                status: options.status || 9, // 9 = Active listings
                uipt: '1,2,3,4,5,6,7,8', // All property types
                v: 8,
                sf: '1,2,3,5,6,7', // Include various home types
                // Optional NYC-focused filters
                ...(options.minPrice && { min_price: options.minPrice }),
                ...(options.maxPrice && { max_price: options.maxPrice }),
                ...(options.minBeds && { min_beds: options.minBeds }),
                ...(options.maxBeds && { max_beds: options.maxBeds }),
                ...(options.minBaths && { min_baths: options.minBaths }),
                ...(options.maxBaths && { max_baths: options.maxBaths })
            };

            console.log(`📊 Fetching NYC CSV data for region ${regionId} (limit: ${params.num_homes})...`);
            
            const response = await this.makeRequest(url, params);
            
            // Parse CSV data
            const csvData = response.data;
            if (!csvData || csvData.length < 50) {
                throw new Error('Received empty or invalid CSV data');
            }
            
            const listings = this.parseCSV(csvData);
            
            if (listings.length === 0) {
                console.log(`⚠️ No listings parsed from CSV data`);
                return [];
            }
            
            console.log(`✅ Successfully scraped ${listings.length} NYC listings from CSV API`);
            return listings;
            
        } catch (error) {
            console.error('❌ Error scraping NYC CSV listings:', error.message);
            
            // Provide helpful error context
            if (error.response?.status === 403) {
                console.log('💡 403 Error suggests anti-bot detection. Try:');
                console.log('   - Waiting longer between requests');
                console.log('   - Using a VPN or different network');
                console.log('   - Running during off-peak hours');
            }
            
            throw error;
        }
    }

    /**
     * Enhanced property details with better error handling
     */
    async getPropertyDetails(propertyUrl) {
        try {
            console.log(`🏠 Getting NYC property details for: ${propertyUrl}`);
            
            // First get the property page HTML to extract IDs
            const response = await this.makeRequest(propertyUrl);
            const html = response.data;
            
            // Check if this is a rental property
            if (propertyUrl.includes('/rent/') || html.includes('rental')) {
                return await this.getRentalDetails(html, propertyUrl);
            } else {
                return await this.getSaleDetails(html, propertyUrl);
            }
            
        } catch (error) {
            console.error(`❌ Error getting NYC property details for ${propertyUrl}:`, error.message);
            throw error;
        }
    }

    /**
     * Extract rental property details using the rentals API
     */
    async getRentalDetails(html, propertyUrl) {
        try {
            // Extract rental ID from og:image meta tag
            const ogImageMatch = html.match(/property="og:image"\s+content="([^"]+)"/);
            if (!ogImageMatch) {
                throw new Error('Could not find og:image meta tag');
            }
            
            const imageUrl = ogImageMatch[1];
            const rentalIdMatch = imageUrl.match(/rent\/([^\/]+)/);
            if (!rentalIdMatch) {
                throw new Error('Could not extract rental ID from image URL');
            }
            
            const rentalId = rentalIdMatch[1];
            console.log(`🏢 Found NYC rental ID: ${rentalId}`);
            
            // Call the rentals API
            const apiUrl = `${this.API_BASE}/v1/rentals/${rentalId}/floorPlans`;
            const apiResponse = await this.makeRequest(apiUrl);
            const rentalData = apiResponse.data;
            
            return {
                type: 'rental',
                url: propertyUrl,
                rentalId: rentalId,
                data: rentalData
            };
            
        } catch (error) {
            console.error('❌ Error getting NYC rental details:', error.message);
            throw error;
        }
    }

    /**
     * Extract sale property details from HTML
     */
    async getSaleDetails(html, propertyUrl) {
        try {
            // Parse key data from HTML using regex patterns
            const extractors = {
                price: /data-rf-test-id="abp-price"[^>]*><div[^>]*>([^<]+)</,
                address: /<div[^>]*class="[^"]*street-address[^"]*"[^>]*>([^<]+)</,
                city: /<div[^>]*class="[^"]*cityStateZip[^"]*"[^>]*>([^<]+)</,
                description: /<div[^>]*id="marketing-remarks-scroll"[^>]*><p[^>]*><span[^>]*>([^<]+)</,
                estimatedPayment: /<span[^>]*class="est-monthly-payment"[^>]*>([^<]+)</
            };
            
            const data = {};
            for (const [key, regex] of Object.entries(extractors)) {
                const match = html.match(regex);
                data[key] = match ? match[1].trim() : null;
            }
            
            // Extract images
            const imageMatches = html.match(/class="[^"]*widenPhoto[^"]*"[^>]*src="([^"]+)"/g) || [];
            data.images = imageMatches.map(match => {
                const srcMatch = match.match(/src="([^"]+)"/);
                return srcMatch ? srcMatch[1] : null;
            }).filter(Boolean);
            
            return {
                type: 'sale',
                url: propertyUrl,
                data: data
            };
            
        } catch (error) {
            console.error('❌ Error parsing NYC sale details:', error.message);
            throw error;
        }
    }

    /**
     * Enhanced property search with better error handling
     */
    async searchProperties(searchTerm, options = {}) {
        try {
            console.log(`🔍 Searching for NYC properties: ${searchTerm}`);
            
            // First get region info
            const region = await this.getRegionId(searchTerm);
            
            // Then get listings for that region with smaller initial limit
            const conservativeOptions = {
                ...options,
                limit: Math.min(options.limit || 50, 100) // Start with smaller batches
            };
            
            const listings = await this.scrapeListingsCSV(region.id, region.type, conservativeOptions);
            
            return {
                region: region,
                listings: listings,
                total: listings.length
            };
            
        } catch (error) {
            console.error(`❌ Error searching NYC properties for ${searchTerm}:`, error.message);
            throw error;
        }
    }

    /**
     * Parse CSV data into structured objects (enhanced)
     */
    parseCSV(csvText) {
        try {
            const lines = csvText.trim().split('\n');
            if (lines.length < 2) {
                console.log('⚠️ CSV has fewer than 2 lines');
                return [];
            }
            
            // Parse header
            const headers = this.parseCSVLine(lines[0]);
            if (headers.length === 0) {
                console.log('⚠️ No headers found in CSV');
                return [];
            }
            
            // Parse data rows
            const listings = [];
            for (let i = 1; i < lines.length; i++) {
                const values = this.parseCSVLine(lines[i]);
                if (values.length === headers.length) {
                    const listing = {};
                    headers.forEach((header, index) => {
                        listing[header.toLowerCase().replace(/[^a-z0-9]/g, '_')] = values[index];
                    });
                    listings.push(listing);
                } else if (values.length > 0) {
                    console.log(`⚠️ Skipping malformed CSV row ${i}: ${values.length} values vs ${headers.length} headers`);
                }
            }
            
            console.log(`📊 Parsed ${listings.length} listings from ${lines.length - 1} CSV rows`);
            return listings;
        } catch (error) {
            console.error('❌ Error parsing CSV:', error.message);
            return [];
        }
    }

    /**
     * Parse a single CSV line handling quoted fields (enhanced)
     */
    parseCSVLine(line) {
        if (!line) return [];
        
        const values = [];
        let current = '';
        let inQuotes = false;
        
        for (let i = 0; i < line.length; i++) {
            const char = line[i];
            
            if (char === '"') {
                if (inQuotes && line[i + 1] === '"') {
                    current += '"';
                    i++; // Skip next quote
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (char === ',' && !inQuotes) {
                values.push(current.trim());
                current = '';
            } else {
                current += char;
            }
        }
        
        values.push(current.trim());
        return values;
    }

    /**
     * Save data to file (enhanced)
     */
    async saveToFile(data, filename) {
        try {
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
            const timestampedFilename = filename.replace('.json', `-${timestamp}.json`);
            
            await fs.writeFile(timestampedFilename, JSON.stringify(data, null, 2));
            console.log(`💾 NYC data saved to ${timestampedFilename}`);
        } catch (error) {
            console.error(`❌ Error saving to file:`, error.message);
        }
    }

    /**
     * Get market data for NYC regions (enhanced)
     */
    async getMarketData(regionId, regionType) {
        try {
            const url = `${this.API_BASE}/market`;
            const params = {
                region_id: regionId,
                region_type: regionType,
                sold_within_days: 90
            };
            
            console.log(`📈 Getting NYC market data for region ${regionId}...`);
            const response = await this.makeRequest(url, params);
            
            let jsonData = response.data;
            if (typeof jsonData === 'string') {
                jsonData = jsonData.replace(/^\)\]\}',?\n?/, '');
                jsonData = JSON.parse(jsonData);
            }
            
            return jsonData;
        } catch (error) {
            console.error('❌ Error getting NYC market data:', error.message);
            throw error;
        }
    }

    /**
     * Enhanced method to get all NYC data with better error handling
     */
    async getAllNYCData(options = {}) {
        console.log('🗽 Fetching data for all NYC boroughs...');
        
        const boroughs = [
            'Manhattan, NY',
            'Brooklyn, NY', 
            'Queens, NY',
            'Bronx, NY',
            'Staten Island, NY'
        ];

        const allData = {};
        const conservativeOptions = {
            ...options,
            limit: Math.min(options.limit || 20, 50) // Very conservative for testing
        };
        
        for (const borough of boroughs) {
            try {
                console.log(`\n📍 Processing ${borough}...`);
                const results = await this.searchProperties(borough, conservativeOptions);
                allData[borough] = results;
                
                // Longer delay between boroughs to be extra respectful
                console.log(`⏰ Waiting 5 seconds before next borough...`);
                await this.delay(5000);
            } catch (error) {
                console.error(`❌ Error processing ${borough}:`, error.message);
                allData[borough] = { error: error.message };
                
                // If we get 403s, wait even longer
                if (error.response?.status === 403) {
                    console.log(`🛑 Got 403 for ${borough}, waiting 10 seconds...`);
                    await this.delay(10000);
                }
            }
        }

        return allData;
    }

    /**
     * Simple test method to check if we can access Redfin at all
     */
    async testConnection() {
        try {
            console.log('🔍 Testing connection to Redfin...');
            const response = await this.makeRequest('https://www.redfin.com/news/data-center');
            console.log(`✅ Connection test successful (${response.status})`);
            return true;
        } catch (error) {
            console.log(`❌ Connection test failed: ${error.message}`);
            return false;
        }
    }
}

// NYC-focused example usage with better error handling
async function nycExample() {
    const scraper = new RedfinAPIScraper();
    
    try {
        console.log('🗽 Starting NYC property scraper...\n');
        
        // First test connection
        const connectionOk = await scraper.testConnection();
        if (!connectionOk) {
            console.log('⚠️ Connection test failed. Check your internet connection.');
            return;
        }
        
        // Try a very conservative test with Manhattan
        console.log('🔍 Testing with small Manhattan search...');
        const manhattanResults = await scraper.searchProperties('Manhattan, NY', {
            limit: 10, // Very small limit for testing
            maxPrice: 2000000,
            status: 9 // Active listings
        });
        
        console.log(`\n📊 Found ${manhattanResults.total} properties in Manhattan`);
        
        if (manhattanResults.total > 0) {
            // If successful, try Brooklyn
            console.log('🔍 Testing Brooklyn...');
            const brooklynResults = await scraper.searchProperties('Brooklyn, NY', {
                limit: 10,
                maxPrice: 1500000
            });
            
            console.log(`📊 Found ${brooklynResults.total} properties in Brooklyn`);
        }
        
        // Save test data
        await scraper.saveToFile({
            testTimestamp: new Date().toISOString(),
            manhattan: manhattanResults,
            status: 'success'
        }, 'nyc-test-results.json');
        
        console.log('\n✅ NYC scraping test completed successfully!');
        
    } catch (error) {
        console.error('💥 NYC scraper test failed:', error.message);
        
        if (error.response?.status === 403) {
            console.log('\n💡 403 Forbidden suggests Redfin is blocking automated requests.');
            console.log('🔧 Try these solutions:');
            console.log('   1. Wait 10-15 minutes before trying again');
            console.log('   2. Use a VPN to change your IP address');
            console.log('   3. Try during off-peak hours (late night/early morning)');
            console.log('   4. Consider using a proxy service like ScrapingBee');
        }
    }
}

// Run if this file is executed directly
if (require.main === module) {
    nycExample().catch(console.error);
}

module.exports = RedfinAPIScraper;
