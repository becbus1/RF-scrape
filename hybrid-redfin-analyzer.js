const axios = require('axios');
const fs = require('fs').promises;
const zlib = require('zlib');
const { promisify } = require('util');

const gunzip = promisify(zlib.gunzip);

class HybridRedfinAnalyzer {
    constructor() {
        // Enhanced HTTP client for targeted scraping
        this.client = axios.create({
            headers: {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-US,en;q=0.9',
                'Cache-Control': 'no-cache',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Referer': 'https://www.redfin.com/'
            },
            timeout: 30000
        });

        // Conservative rate limiting for targeted scraping
        this.rateLimitDelay = 2000; // 2 seconds between description fetches

        // NYC-specific distress signal keywords
        this.distressKeywords = [
            'motivated seller', 'must sell', 'as-is', 'as is', 'fixer-upper', 
            'fixer upper', 'handyman special', 'tlc', 'needs work', 'needs updating',
            'estate sale', 'inherited', 'probate', 'divorce', 'foreclosure',
            'short sale', 'bank owned', 'reo', 'price reduced', 'reduced price',
            'bring offers', 'all offers considered', 'make offer', 'obo',
            'cash only', 'investor special', 'diamond in the rough',
            'potential', 'opportunity', 'priced to sell', 'quick sale',
            // NYC-specific terms
            'prewar', 'war building', 'walk up', 'no elevator', 'gut renovation needed',
            'tenant occupied', 'rent stabilized', 'co-op conversion', 'sponsor unit'
        ];
        
        // NYC-specific warning signals
        this.warningKeywords = [
            'flood', 'water damage', 'foundation issues', 'structural',
            'fire damage', 'mold', 'asbestos', 'lead paint', 'septic',
            'well water', 'no permits', 'unpermitted', 'easement',
            'hoa issues', 'back taxes', 'liens', 'title issues',
            // NYC-specific warnings
            'no board approval', 'flip tax', 'assessment pending', 'rent controlled',
            'certificate of occupancy', 'housing court', 'rent stabilized tenant',
            'basement apartment', 'illegal conversion', 'landmark building'
        ];

        // Redfin public data URLs
        this.publicDataUrls = {
            cityTracker: 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/city_market_tracker.tsv000.gz',
            zipTracker: 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/zip_market_tracker.tsv000.gz',
            neighborhoodTracker: 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/neighborhood_market_tracker.tsv000.gz',
            weeklyInventory: 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/weekly_housing_inventory_core_metro_us.tsv000.gz'
        };

        // NYC area identifiers for filtering public data
        this.nycIdentifiers = {
            cities: ['New York', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island'],
            state: 'NY',
            metros: ['New York, NY'],
            zipPrefixes: ['100', '101', '102', '103', '104', '112', '113', '114', '116']
        };
    }

    async delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    /**
     * STEP 1: Download and parse Redfin's weekly public data
     */
    async downloadPublicData() {
        console.log('📊 Downloading Redfin public data...');
        
        const datasets = {};
        
        for (const [name, url] of Object.entries(this.publicDataUrls)) {
            try {
                console.log(`📥 Downloading ${name}...`);
                
                const response = await axios.get(url, {
                    responseType: 'arraybuffer',
                    headers: {
                        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
                    }
                });

                // Decompress gzipped data
                const decompressed = await gunzip(response.data);
                const tsvData = decompressed.toString('utf-8');
                
                // Parse TSV to objects
                datasets[name] = this.parseTSV(tsvData);
                console.log(`✅ ${name}: ${datasets[name].length} records`);
                
            } catch (error) {
                console.error(`❌ Error downloading ${name}:`, error.message);
                datasets[name] = [];
            }
        }

        return datasets;
    }

    /**
     * Parse TSV data into objects
     */
    parseTSV(tsvData) {
        const lines = tsvData.trim().split('\n');
        if (lines.length < 2) return [];

        const headers = lines[0].split('\t');
        const records = [];

        for (let i = 1; i < lines.length; i++) {
            const values = lines[i].split('\t');
            if (values.length === headers.length) {
                const record = {};
                headers.forEach((header, index) => {
                    record[header.trim()] = values[index].trim();
                });
                records.push(record);
            }
        }

        return records;
    }

    /**
     * STEP 2: Filter public data for NYC properties only
     */
    filterNYCData(datasets) {
        console.log('🗽 Filtering for NYC properties...');
        
        const nycData = {
            cityData: [],
            zipData: [],
            neighborhoodData: [],
            inventoryData: []
        };

        // Filter city data
        nycData.cityData = datasets.cityTracker.filter(record => 
            this.nycIdentifiers.cities.includes(record.city) && 
            record.state_code === this.nycIdentifiers.state
        );

        // Filter ZIP data
        nycData.zipData = datasets.zipTracker.filter(record => 
            record.state_code === this.nycIdentifiers.state &&
            this.nycIdentifiers.zipPrefixes.some(prefix => 
                record.region_name?.startsWith(prefix)
            )
        );

        // Filter neighborhood data
        nycData.neighborhoodData = datasets.neighborhoodTracker.filter(record => 
            this.nycIdentifiers.cities.includes(record.city) && 
            record.state_code === this.nycIdentifiers.state
        );

        // Filter inventory data
        nycData.inventoryData = datasets.weeklyInventory.filter(record => 
            this.nycIdentifiers.metros.includes(record.cbsa_title)
        );

        console.log('✅ NYC Data Summary:');
        console.log(`   City records: ${nycData.cityData.length}`);
        console.log(`   ZIP records: ${nycData.zipData.length}`);
        console.log(`   Neighborhood records: ${nycData.neighborhoodData.length}`);
        console.log(`   Inventory records: ${nycData.inventoryData.length}`);

        return nycData;
    }

    /**
     * STEP 3: Calculate market averages for pricing analysis
     */
    calculateMarketAverages(nycData) {
        console.log('📈 Calculating NYC market averages...');
        
        const marketAverages = {
            cityLevel: {},
            zipLevel: {},
            neighborhoodLevel: {}
        };

        // Calculate city-level averages
        nycData.cityData.forEach(record => {
            const city = record.city;
            const pricePerSqft = parseFloat(record.avg_sale_to_list_price);
            const medianSalePrice = parseFloat(record.median_sale_price);
            
            if (!isNaN(pricePerSqft) && !isNaN(medianSalePrice)) {
                marketAverages.cityLevel[city] = {
                    avgPricePerSqft: pricePerSqft,
                    medianSalePrice: medianSalePrice,
                    avgDaysOnMarket: parseFloat(record.avg_days_on_market) || 0,
                    inventoryCount: parseInt(record.homes_sold) || 0
                };
            }
        });

        // Calculate ZIP-level averages
        nycData.zipData.forEach(record => {
            const zip = record.region_name;
            const pricePerSqft = parseFloat(record.avg_sale_to_list_price);
            const medianSalePrice = parseFloat(record.median_sale_price);
            
            if (!isNaN(pricePerSqft) && !isNaN(medianSalePrice)) {
                marketAverages.zipLevel[zip] = {
                    avgPricePerSqft: pricePerSqft,
                    medianSalePrice: medianSalePrice,
                    avgDaysOnMarket: parseFloat(record.avg_days_on_market) || 0,
                    inventoryCount: parseInt(record.homes_sold) || 0
                };
            }
        });

        // Calculate neighborhood-level averages
        nycData.neighborhoodData.forEach(record => {
            const neighborhood = `${record.region_name}, ${record.city}`;
            const pricePerSqft = parseFloat(record.avg_sale_to_list_price);
            const medianSalePrice = parseFloat(record.median_sale_price);
            
            if (!isNaN(pricePerSqft) && !isNaN(medianSalePrice)) {
                marketAverages.neighborhoodLevel[neighborhood] = {
                    avgPricePerSqft: pricePerSqft,
                    medianSalePrice: medianSalePrice,
                    avgDaysOnMarket: parseFloat(record.avg_days_on_market) || 0,
                    inventoryCount: parseInt(record.homes_sold) || 0
                };
            }
        });

        console.log('✅ Market Averages Calculated:');
        console.log(`   Cities: ${Object.keys(marketAverages.cityLevel).length}`);
        console.log(`   ZIP codes: ${Object.keys(marketAverages.zipLevel).length}`);
        console.log(`   Neighborhoods: ${Object.keys(marketAverages.neighborhoodLevel).length}`);

        return marketAverages;
    }

    /**
     * STEP 4: Find undervalued properties from public data
     */
    findUndervaluedProperties(nycData, marketAverages, criteria = {}) {
        console.log('🎯 Identifying undervalued properties...');
        
        const {
            minDiscountPercent = 15,
            maxDaysOnMarket = 90,
            minPrice = 200000,
            maxPrice = 3000000
        } = criteria;

        const undervaluedProperties = [];

        // Analyze properties from all data sources
        const allProperties = [
            ...nycData.cityData,
            ...nycData.zipData,
            ...nycData.neighborhoodData
        ];

        allProperties.forEach(record => {
            try {
                const listingPrice = parseFloat(record.median_list_price || record.avg_list_price);
                const salePrice = parseFloat(record.median_sale_price || record.avg_sale_price);
                const daysOnMarket = parseFloat(record.avg_days_on_market);
                
                if (isNaN(listingPrice) || isNaN(salePrice) || 
                    listingPrice < minPrice || listingPrice > maxPrice ||
                    daysOnMarket > maxDaysOnMarket) {
                    return;
                }

                // Find best market comparable
                const marketComparable = this.findBestComparable(record, marketAverages);
                if (!marketComparable) return;

                // Calculate how much below market
                const expectedPrice = marketComparable.medianSalePrice;
                const actualPrice = listingPrice;
                const priceDifference = expectedPrice - actualPrice;
                const percentBelowMarket = (priceDifference / expectedPrice) * 100;

                if (percentBelowMarket >= minDiscountPercent) {
                    undervaluedProperties.push({
                        // Property details
                        location: record.city || record.region_name,
                        zip: record.region_name,
                        listingPrice: listingPrice,
                        expectedPrice: expectedPrice,
                        percentBelowMarket: percentBelowMarket,
                        daysOnMarket: daysOnMarket,
                        
                        // Market context
                        marketComparable: marketComparable,
                        comparisonLevel: this.getComparisonLevel(record, marketAverages),
                        
                        // For later enhancement
                        needsDescriptionScraping: true,
                        distressSignals: [],
                        warningTags: [],
                        finalScore: 0,
                        reasoning: ''
                    });
                }
            } catch (error) {
                // Skip malformed records
            }
        });

        // Sort by percentage below market (best deals first)
        undervaluedProperties.sort((a, b) => b.percentBelowMarket - a.percentBelowMarket);

        console.log(`✅ Found ${undervaluedProperties.length} potentially undervalued properties`);
        console.log(`   Criteria: ${minDiscountPercent}%+ below market, max ${maxDaysOnMarket} days`);

        return undervaluedProperties;
    }

    /**
     * Find best market comparable for a property
     */
    findBestComparable(record, marketAverages) {
        // Try neighborhood first, then ZIP, then city
        const neighborhood = `${record.region_name}, ${record.city}`;
        const zip = record.region_name;
        const city = record.city;

        if (marketAverages.neighborhoodLevel[neighborhood]) {
            return marketAverages.neighborhoodLevel[neighborhood];
        } else if (marketAverages.zipLevel[zip]) {
            return marketAverages.zipLevel[zip];
        } else if (marketAverages.cityLevel[city]) {
            return marketAverages.cityLevel[city];
        }

        return null;
    }

    /**
     * Get comparison level for scoring
     */
    getComparisonLevel(record, marketAverages) {
        const neighborhood = `${record.region_name}, ${record.city}`;
        const zip = record.region_name;
        const city = record.city;

        if (marketAverages.neighborhoodLevel[neighborhood]) {
            return 'Neighborhood';
        } else if (marketAverages.zipLevel[zip]) {
            return 'ZIP Code';
        } else if (marketAverages.cityLevel[city]) {
            return 'City';
        }

        return 'Unknown';
    }

    /**
     * STEP 5: Enhance top candidates with property descriptions
     */
    async enhanceWithDescriptions(undervaluedProperties) {
        console.log(`🔍 Enhancing ${undervaluedProperties.length} properties with descriptions...`);
        
        const enhancedProperties = [];
        let successCount = 0;
        let failCount = 0;

        for (let i = 0; i < undervaluedProperties.length; i++) {
            const property = undervaluedProperties[i];
            
            try {
                console.log(`📝 Fetching description ${i + 1}/${undervaluedProperties.length}: ${property.location}`);
                
                // Simulate property URL (in real implementation, you'd get this from public data or search)
                const description = await this.fetchPropertyDescription(property);
                
                if (description) {
                    // Analyze description for keywords
                    property.distressSignals = this.findDistressSignals(description);
                    property.warningTags = this.findWarningSignals(description);
                    property.description = description;
                    
                    // Calculate final score
                    property.finalScore = this.calculateFinalScore(property);
                    property.reasoning = this.generateReasoning(property);
                    
                    successCount++;
                } else {
                    // No description available, use quantitative score only
                    property.finalScore = this.calculateQuantitativeScore(property);
                    property.reasoning = this.generateQuantitativeReasoning(property);
                    failCount++;
                }

                enhancedProperties.push(property);

                // Rate limiting
                await this.delay(this.rateLimitDelay);
                
            } catch (error) {
                console.error(`❌ Error enhancing ${property.location}:`, error.message);
                
                // Add property without description enhancement
                property.finalScore = this.calculateQuantitativeScore(property);
                property.reasoning = this.generateQuantitativeReasoning(property);
                enhancedProperties.push(property);
                failCount++;

                // If getting 403s, slow down
                if (error.response?.status === 403) {
                    console.log('⏰ Got 403, slowing down...');
                    await this.delay(5000);
                }
            }
        }

        // Sort by final score
        enhancedProperties.sort((a, b) => b.finalScore - a.finalScore);

        console.log(`✅ Enhancement complete:`);
        console.log(`   Successful descriptions: ${successCount}`);
        console.log(`   Failed/no description: ${failCount}`);
        console.log(`   Total properties: ${enhancedProperties.length}`);

        return enhancedProperties;
    }

    /**
     * Fetch property description (placeholder - would need actual property URLs)
     */
    async fetchPropertyDescription(property) {
        // NOTE: This is a placeholder. In reality, you'd need to:
        // 1. Get property URLs from public data or search
        // 2. Scrape individual property pages
        // 3. Extract description text
        
        // For now, simulate some properties having descriptions
        if (Math.random() > 0.3) { // 70% success rate simulation
            const sampleDescriptions = [
                'Beautiful pre-war building with original details. Motivated seller looking for quick close.',
                'Charming brownstone in need of TLC. Great potential for the right buyer.',
                'Recently renovated with modern amenities. Priced to sell quickly.',
                'Estate sale - must be sold as-is. Cash offers preferred.',
                'Investor special! Needs work but great bones. All offers considered.',
                'Rent-stabilized tenant in place. Perfect for investors.',
                'No board approval required. Move right in!'
            ];
            
            return sampleDescriptions[Math.floor(Math.random() * sampleDescriptions.length)];
        }
        
        return null; // Simulate failed description fetch
    }

    /**
     * Find distress signals in description
     */
    findDistressSignals(description) {
        const text = description.toLowerCase();
        return this.distressKeywords.filter(keyword => 
            text.includes(keyword.toLowerCase())
        );
    }

    /**
     * Find warning signals in description
     */
    findWarningSignals(description) {
        const text = description.toLowerCase();
        return this.warningKeywords.filter(keyword => 
            text.includes(keyword.toLowerCase())
        );
    }

    /**
     * Calculate final score with description enhancement
     */
    calculateFinalScore(property) {
        let score = 0;

        // Base score from percentage below market (0-50 points)
        const percentScore = Math.min(property.percentBelowMarket * 2, 50);
        score += percentScore;

        // Days on market scoring (0-20 points)
        let domScore = 0;
        if (property.daysOnMarket <= 7) {
            domScore = 20;
        } else if (property.daysOnMarket <= 30) {
            domScore = 15;
        } else if (property.daysOnMarket <= 60) {
            domScore = 10;
        } else {
            domScore = 5;
        }
        score += domScore;

        // Distress signals bonus (0-15 points)
        const distressScore = Math.min(property.distressSignals.length * 3, 15);
        score += distressScore;

        // Comparison quality bonus (0-10 points)
        let compScore = 0;
        if (property.comparisonLevel === 'Neighborhood') {
            compScore = 10;
        } else if (property.comparisonLevel === 'ZIP Code') {
            compScore = 7;
        } else if (property.comparisonLevel === 'City') {
            compScore = 5;
        }
        score += compScore;

        // Warning signals penalty (0 to -10 points)
        const warningPenalty = Math.min(property.warningTags.length * 2, 10);
        score -= warningPenalty;

        return Math.round(score);
    }

    /**
     * Calculate quantitative score only (no description)
     */
    calculateQuantitativeScore(property) {
        let score = 0;

        // Base score from percentage below market (0-50 points)
        const percentScore = Math.min(property.percentBelowMarket * 2, 50);
        score += percentScore;

        // Days on market scoring (0-20 points)
        let domScore = 0;
        if (property.daysOnMarket <= 7) {
            domScore = 20;
        } else if (property.daysOnMarket <= 30) {
            domScore = 15;
        } else if (property.daysOnMarket <= 60) {
            domScore = 10;
        } else {
            domScore = 5;
        }
        score += domScore;

        // Comparison quality bonus (0-10 points)
        let compScore = 0;
        if (property.comparisonLevel === 'Neighborhood') {
            compScore = 10;
        } else if (property.comparisonLevel === 'ZIP Code') {
            compScore = 7;
        } else if (property.comparisonLevel === 'City') {
            compScore = 5;
        }
        score += compScore;

        return Math.round(score);
    }

    /**
     * Generate reasoning for final score
     */
    generateReasoning(property) {
        const reasons = [];

        reasons.push(`${property.percentBelowMarket.toFixed(1)}% below market (+${Math.min(property.percentBelowMarket * 2, 50).toFixed(1)} pts)`);
        
        if (property.daysOnMarket <= 7) {
            reasons.push(`Fresh listing (${property.daysOnMarket} days) (+20 pts)`);
        } else if (property.daysOnMarket <= 30) {
            reasons.push(`Recent listing (${property.daysOnMarket} days) (+15 pts)`);
        } else {
            reasons.push(`Older listing (${property.daysOnMarket} days) (+10 pts)`);
        }

        if (property.distressSignals.length > 0) {
            reasons.push(`${property.distressSignals.length} distress signals: ${property.distressSignals.join(', ')} (+${Math.min(property.distressSignals.length * 3, 15)} pts)`);
        }

        reasons.push(`${property.comparisonLevel} comparison (+${property.comparisonLevel === 'Neighborhood' ? 10 : property.comparisonLevel === 'ZIP Code' ? 7 : 5} pts)`);

        if (property.warningTags.length > 0) {
            reasons.push(`${property.warningTags.length} warnings: ${property.warningTags.join(', ')} (-${Math.min(property.warningTags.length * 2, 10)} pts)`);
        }

        return reasons.join('; ');
    }

    /**
     * Generate reasoning for quantitative-only score
     */
    generateQuantitativeReasoning(property) {
        const reasons = [];

        reasons.push(`${property.percentBelowMarket.toFixed(1)}% below market`);
        reasons.push(`${property.daysOnMarket} days on market`);
        reasons.push(`${property.comparisonLevel} level comparison`);
        reasons.push('(No description available for keyword analysis)');

        return reasons.join('; ');
    }

    /**
     * Format results for database storage
     */
    formatForDatabase(properties) {
        return properties.map(property => ({
            address: property.location,
            price: `$${property.listingPrice.toLocaleString()}`,
            beds: null, // Not available in public data
            sqft: null, // Not available in public data
            zip: property.zip,
            link: null, // Would need to be populated from property URLs
            score: property.finalScore,
            percent_below_market: property.percentBelowMarket,
            warning_tags: property.warningTags || []
        }));
    }

    /**
     * Save results to file
     */
    async saveResults(data, filename) {
        try {
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
            const timestampedFilename = filename.replace('.json', `-${timestamp}.json`);
            
            await fs.writeFile(timestampedFilename, JSON.stringify(data, null, 2));
            console.log(`💾 Results saved to ${timestampedFilename}`);
        } catch (error) {
            console.error(`❌ Error saving results:`, error.message);
        }
    }

    /**
     * MAIN METHOD: Complete hybrid analysis
     */
    async runCompleteAnalysis(criteria = {}) {
        console.log('🚀 Starting Hybrid Redfin Analysis...\n');
        
        try {
            // Step 1: Download public data
            const publicData = await this.downloadPublicData();
            
            // Step 2: Filter for NYC
            const nycData = this.filterNYCData(publicData);
            
            // Step 3: Calculate market averages
            const marketAverages = this.calculateMarketAverages(nycData);
            
            // Step 4: Find undervalued properties
            const undervaluedProperties = this.findUndervaluedProperties(nycData, marketAverages, criteria);
            
            if (undervaluedProperties.length === 0) {
                console.log('😔 No undervalued properties found with current criteria');
                return { undervaluedProperties: [], summary: { total: 0 } };
            }
            
            // Step 5: Enhance with descriptions
            const enhancedProperties = await this.enhanceWithDescriptions(undervaluedProperties);
            
            // Prepare final results
            const results = {
                timestamp: new Date().toISOString(),
                criteria: criteria,
                summary: {
                    totalAnalyzed: enhancedProperties.length,
                    avgPercentBelowMarket: enhancedProperties.reduce((sum, p) => sum + p.percentBelowMarket, 0) / enhancedProperties.length,
                    avgScore: enhancedProperties.reduce((sum, p) => sum + p.finalScore, 0) / enhancedProperties.length,
                    withDescriptions: enhancedProperties.filter(p => p.description).length,
                    withDistressSignals: enhancedProperties.filter(p => p.distressSignals.length > 0).length
                },
                undervaluedProperties: enhancedProperties,
                marketAverages: marketAverages
            };

            // Save results
            await this.saveResults(results, 'hybrid-analysis-results.json');
            await this.saveResults(this.formatForDatabase(enhancedProperties), 'hybrid-analysis-db.json');

            console.log('\n🎉 Hybrid analysis complete!');
            console.log(`📊 Found ${enhancedProperties.length} undervalued properties`);
            console.log(`💾 Results saved to files`);

            return results;

        } catch (error) {
            console.error('❌ Hybrid analysis failed:', error.message);
            throw error;
        }
    }
}

// Example usage
async function runHybridAnalysis() {
    const analyzer = new HybridRedfinAnalyzer();
    
    try {
        const results = await analyzer.runCompleteAnalysis({
            minDiscountPercent: 15,  // 15% below market minimum
            maxDaysOnMarket: 90,     // Listed within 90 days
            minPrice: 300000,        // $300K minimum for NYC
            maxPrice: 2500000        // $2.5M maximum
        });

        // Display top results
        console.log('\n🏆 TOP UNDERVALUED NYC PROPERTIES:');
        console.log('='.repeat(60));

        results.undervaluedProperties.slice(0, 10).forEach((property, index) => {
            console.log(`\n📊 ANALYSIS SUMMARY:`);
        console.log(`✅ Total undervalued properties: ${results.summary.totalAnalyzed}`);
        console.log(`📈 Average % below market: ${results.summary.avgPercentBelowMarket.toFixed(1)}%`);
        console.log(`🏆 Average score: ${results.summary.avgScore.toFixed(1)}/100`);
        console.log(`📝 Properties with descriptions: ${results.summary.withDescriptions}`);
        console.log(`🚨 Properties with distress signals: ${results.summary.withDistressSignals}`);

    } catch (error) {
        console.error('💥 Analysis failed:', error.message);
    }
}

// Run if executed directly
if (require.main === module) {
    runHybridAnalysis().catch(console.error);
}

module.exports = HybridRedfinAnalyzer;${index + 1}. 📍 ${property.location}`);
            console.log(`   💰 Listed: $${property.listingPrice.toLocaleString()} (${property.percentBelowMarket.toFixed(1)}% below market)`);
            console.log(`   📊 Expected: $${property.expectedPrice.toLocaleString()}`);
            console.log(`   🏆 Score: ${property.finalScore}/100`);
            console.log(`   📅 ${property.daysOnMarket} days on market`);
            
            if (property.distressSignals.length > 0) {
                console.log(`   🚨 Distress signals: ${property.distressSignals.join(', ')}`);
            }
            
            if (property.warningTags.length > 0) {
                console.log(`   ⚠️ Warnings: ${property.warningTags.join(', ')}`);
            }
            
            console.log(`   🧠 Reasoning: ${property.reasoning}`);
        });

        console.log(`\n
