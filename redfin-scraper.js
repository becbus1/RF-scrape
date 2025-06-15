const axios = require('axios');
const fs = require('fs').promises;

class RedfinAPIScraper {
    constructor() {
        // Set up HTTP client with browser-like headers to avoid detection
        this.client = axios.create({
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-US,en;q=0.9',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
                'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"Windows"',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Upgrade-Insecure-Requests': '1'
            },
            timeout: 30000,
            maxRedirects: 5
        });

        // Common API base URLs discovered from network analysis
        this.API_BASE = 'https://www.redfin.com/stingray/api';
        
        // Rate limiting to be respectful
        this.rateLimitDelay = 1000; // 1 second between requests

        // NYC-specific region mappings (discovered through testing)
        this.NYC_REGIONS = {
            'Manhattan, NY': { id: 16904, type: 6, name: 'Manhattan' },
            'Brooklyn, NY': { id: 17072, type: 6, name: 'Brooklyn' },
            'Queens, NY': { id: 17085, type: 6, name: 'Queens' },
            'Bronx, NY': { id: 17070, type: 6, name: 'Bronx' },
            'Staten Island, NY': { id: 17112, type: 6, name: 'Staten Island' },
            'New York, NY': { id: 30749, type: 6, name: 'New York City' }
        };
    }

    async delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    /**
     * Get region ID for NYC locations with cached mappings
     */
    async getRegionId(location) {
        try {
            // Check if it's a known NYC region first
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
            const response = await this.client.get(url, { params });
            
            // Redfin returns data with a safety prefix, remove it
            const jsonData = response.data.replace(/^\)\]\}',?\n?/, '');
            const data = JSON.parse(jsonData);
            
            if (data.payload && data.payload.exactMatch) {
                const region = data.payload.exactMatch;
                console.log(`✅ Found region: ${region.name} (ID: ${region.id}, Type: ${region.type})`);
                return {
                    id: region.id,
                    type: region.type,
                    name: region.name
                };
            }
            
            throw new Error('No exact match found for location');
        } catch (error) {
            console.error(`❌ Error getting region ID for ${location}:`, error.message);
            throw error;
        }
    }

    /**
     * Scrape property listings using Redfin's GIS CSV API - NYC optimized
     */
    async scrapeListingsCSV(regionId, regionType, options = {}) {
        try {
            const url = `${this.API_BASE}/gis-csv`;
            
            // NYC-optimized parameters
            const params = {
                al: 1, // Include all listings
                market: 'newyork', // NYC market
                num_homes: options.limit || 350,
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

            console.log(`📊 Fetching NYC CSV data for region ${regionId}...`);
            await this.delay(this.rateLimitDelay);
            
            const response = await this.client.get(url, { params });
            
            // Parse CSV data
            const csvData = response.data;
            const listings = this.parseCSV(csvData);
            
            console.log(`✅ Successfully scraped ${listings.length} NYC listings from CSV API`);
            return listings;
            
        } catch (error) {
            console.error('❌ Error scraping NYC CSV listings:', error.message);
            throw error;
        }
    }

    /**
     * Get detailed property information for NYC properties
     */
    async getPropertyDetails(propertyUrl) {
        try {
            console.log(`🏠 Getting NYC property details for: ${propertyUrl}`);
            
            // First get the property page HTML to extract IDs
            await this.delay(this.rateLimitDelay);
            const htmlResponse = await this.client.get(propertyUrl);
            const html = htmlResponse.data;
            
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
            await this.delay(this.rateLimitDelay);
            
            const apiResponse = await this.client.get(apiUrl);
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
     * Search for NYC properties using Redfin's search API
     */
    async searchProperties(searchTerm, options = {}) {
        try {
            console.log(`🔍 Searching for NYC properties: ${searchTerm}`);
            
            // First get region info
            const region = await this.getRegionId(searchTerm);
            
            // Then get listings for that region
            const listings = await this.scrapeListingsCSV(region.id, region.type, options);
            
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
     * Parse CSV data into structured objects
     */
    parseCSV(csvText) {
        const lines = csvText.trim().split('\n');
        if (lines.length < 2) return [];
        
        // Parse header
        const headers = this.parseCSVLine(lines[0]);
        
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
            }
        }
        
        return listings;
    }

    /**
     * Parse a single CSV line handling quoted fields
     */
    parseCSVLine(line) {
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
     * Save data to file
     */
    async saveToFile(data, filename) {
        try {
            await fs.writeFile(filename, JSON.stringify(data, null, 2));
            console.log(`💾 NYC data saved to ${filename}`);
        } catch (error) {
            console.error(`❌ Error saving to file:`, error.message);
        }
    }

    /**
     * Get market data for NYC regions
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
            await this.delay(this.rateLimitDelay);
            
            const response = await this.client.get(url, { params });
            const jsonData = response.data.replace(/^\)\]\}',?\n?/, '');
            
            return JSON.parse(jsonData);
        } catch (error) {
            console.error('❌ Error getting NYC market data:', error.message);
            throw error;
        }
    }

    /**
     * Get all NYC boroughs data in one call
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
        
        for (const borough of boroughs) {
            try {
                console.log(`\n📍 Processing ${borough}...`);
                const results = await this.searchProperties(borough, options);
                allData[borough] = results;
                
                // Rate limiting between boroughs
                await this.delay(2000);
            } catch (error) {
                console.error(`❌ Error processing ${borough}:`, error.message);
                allData[borough] = { error: error.message };
            }
        }

        return allData;
    }
}

// NYC-focused example usage
async function nycExample() {
    const scraper = new RedfinAPIScraper();
    
    try {
        console.log('🗽 Starting NYC property scraper...\n');
        
        // Get Manhattan properties under $2M
        const manhattanResults = await scraper.searchProperties('Manhattan, NY', {
            limit: 100,
            maxPrice: 2000000,
            status: 9 // Active listings
        });
        
        console.log(`\n📊 Found ${manhattanResults.total} properties in Manhattan`);
        
        // Get all NYC boroughs data
        const allNYC = await scraper.getAllNYCData({
            limit: 50,
            maxPrice: 1500000
        });
        
        // Save NYC data
        await scraper.saveToFile(allNYC, 'nyc-properties.json');
        
        console.log('\n✅ NYC scraping completed successfully!');
        
    } catch (error) {
        console.error('💥 NYC scraper failed:', error.message);
    }
}

// Run if this file is executed directly
if (require.main === module) {
    nycExample().catch(console.error);
}

module.exports = RedfinAPIScraper;
