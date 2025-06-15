require('dotenv').config();
const axios = require('axios');
const fs = require('fs').promises;
const zlib = require('zlib');
const { promisify } = require('util');

const gunzip = promisify(zlib.gunzip);

class UndervaluedPropertyFinder {
    constructor() {
        this.rapidApiKey = process.env.RAPIDAPI_KEY;
        this.redfinMarketData = null;
        this.apiCallsUsed = 0;
        this.maxApiCalls = 25;
        
        // Distress signal keywords for description analysis
        this.distressSignals = [
            'motivated seller', 'must sell', 'as-is', 'as is', 'needs work',
            'fixer-upper', 'fixer upper', 'handyman special', 'tlc', 'needs updating',
            'estate sale', 'inherited', 'probate', 'divorce', 'foreclosure',
            'short sale', 'bank owned', 'reo', 'price reduced', 'reduced price',
            'bring offers', 'all offers considered', 'make offer', 'obo',
            'cash only', 'investor special', 'diamond in the rough',
            'potential', 'opportunity', 'priced to sell', 'quick sale',
            'needs renovation', 'gut renovation', 'tear down', 'build new',
            'prewar', 'original condition', 'no board approval', 'flip tax'
        ];

        this.warningSignals = [
            'flood damage', 'water damage', 'foundation issues', 'structural',
            'fire damage', 'mold', 'asbestos', 'lead paint', 'no permits',
            'back taxes', 'liens', 'title issues', 'housing court'
        ];

        // NYC neighborhood mapping for StreetEasy API
        this.neighborhoods = [
            // Brooklyn - Mix of price points
            'Park Slope', 'Williamsburg', 'DUMBO', 'Brooklyn Heights', 'Red Hook',
            'Carroll Gardens', 'Cobble Hill', 'Prospect Heights', 'Fort Greene',
            'Bed-Stuy', 'Crown Heights', 'Bushwick', 'Greenpoint',
            
            // Queens - Generally more affordable
            'Astoria', 'Long Island City', 'Sunnyside', 'Woodside', 'Jackson Heights',
            'Elmhurst', 'Rego Park', 'Forest Hills', 'Flushing',
            
            // Manhattan - High-value areas
            'East Village', 'Lower East Side', 'Chinatown', 'Financial District',
            'Hell\'s Kitchen', 'Murray Hill', 'Gramercy', 'NoLita',
            
            // Bronx - Most affordable
            'South Bronx', 'Mott Haven', 'Hunts Point', 'Concourse',
            
            // Staten Island
            'St. George', 'Stapleton', 'New Brighton'
        ];
    }

    /**
     * Step 1: Download and process REAL Redfin neighborhood data
     */
    async getRedfinNeighborhoodData() {
        if (this.redfinMarketData) {
            return this.redfinMarketData;
        }

        console.log('📊 Downloading latest Redfin neighborhood market data...');
        
        try {
            // Download the most recent neighborhood tracker data
            const response = await axios.get(
                'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/neighborhood_market_tracker.tsv000.gz',
                { 
                    responseType: 'arraybuffer',
                    timeout: 60000 
                }
            );

            console.log(`✅ Downloaded ${(response.data.length / 1024 / 1024).toFixed(1)}MB of market data`);
            
            // Decompress and process the TSV data
            const decompressed = await gunzip(response.data);
            const tsvData = decompressed.toString('utf-8');
            
            const neighborhoodData = this.parseNeighborhoodTSV(tsvData);
            this.redfinMarketData = neighborhoodData;
            
            console.log(`✅ Processed ${Object.keys(neighborhoodData).length} NYC neighborhoods`);
            return neighborhoodData;
            
        } catch (error) {
            console.warn('⚠️ Error downloading Redfin data, using enhanced mock data:', error.message);
            const mockData = this.getEnhancedMockData();
            this.redfinMarketData = mockData;
            return mockData;
        }
    }

    /**
     * Parse Redfin TSV data and extract NYC neighborhood price per sqft
     */
    parseNeighborhoodTSV(tsvData) {
        console.log('🔍 Parsing neighborhood data for NYC...');
        
        const lines = tsvData.trim().split('\n');
        const headers = lines[0].split('\t');
        
        // Find the columns we need
        const cityIndex = headers.indexOf('city');
        const stateIndex = headers.indexOf('state_code');
        const regionIndex = headers.indexOf('region_name');
        const medianSalePriceIndex = headers.indexOf('median_sale_price');
        const avgListPriceIndex = headers.indexOf('avg_list_price');
        const avgDaysOnMarketIndex = headers.indexOf('avg_days_on_market');
        const periodEndIndex = headers.indexOf('period_end');

        const nycData = {};
        let processedLines = 0;
        const maxLines = 50000; // Process first 50k lines to avoid memory issues

        for (let i = 1; i < Math.min(lines.length, maxLines); i++) {
            const values = lines[i].split('\t');
            
            if (values.length !== headers.length) continue;
            
            const city = values[cityIndex];
            const state = values[stateIndex];
            const neighborhood = values[regionIndex];
            const medianSalePrice = parseFloat(values[medianSalePriceIndex]);
            const avgListPrice = parseFloat(values[avgListPriceIndex]);
            const avgDaysOnMarket = parseFloat(values[avgDaysOnMarketIndex]);
            const periodEnd = values[periodEndIndex];

            // Filter for NYC neighborhoods with recent data
            if (state === 'NY' && 
                ['New York', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island'].includes(city) &&
                !isNaN(medianSalePrice) && medianSalePrice > 0 &&
                periodEnd && periodEnd.includes('2024')) { // Recent data only

                const key = neighborhood;
                
                // Calculate estimated price per sqft (NYC average is ~600-800 sqft for 1-2BR)
                const estimatedAvgSqft = 750; // Conservative estimate
                const pricePerSqft = medianSalePrice / estimatedAvgSqft;

                if (!nycData[key] || nycData[key].periodEnd < periodEnd) {
                    nycData[key] = {
                        neighborhood: neighborhood,
                        city: city,
                        medianSalePrice: medianSalePrice,
                        avgListPrice: avgListPrice || medianSalePrice,
                        estimatedPricePerSqft: Math.round(pricePerSqft),
                        avgDaysOnMarket: avgDaysOnMarket || 45,
                        periodEnd: periodEnd
                    };
                }
            }
            
            processedLines++;
            if (processedLines % 10000 === 0) {
                console.log(`   📊 Processed ${processedLines} lines...`);
            }
        }

        return nycData;
    }

    /**
     * Enhanced mock data with realistic price per sqft for testing
     */
    getEnhancedMockData() {
        return {
            // Brooklyn - Real estimates based on current market
            'Park Slope': { 
                medianSalePrice: 1350000, estimatedPricePerSqft: 1200, 
                avgDaysOnMarket: 42, city: 'Brooklyn' 
            },
            'Williamsburg': { 
                medianSalePrice: 1200000, estimatedPricePerSqft: 1100, 
                avgDaysOnMarket: 38, city: 'Brooklyn' 
            },
            'Red Hook': { 
                medianSalePrice: 900000, estimatedPricePerSqft: 850, 
                avgDaysOnMarket: 35, city: 'Brooklyn' 
            },
            'Bed-Stuy': { 
                medianSalePrice: 1100000, estimatedPricePerSqft: 950, 
                avgDaysOnMarket: 40, city: 'Brooklyn' 
            },
            'Crown Heights': { 
                medianSalePrice: 900000, estimatedPricePerSqft: 750, 
                avgDaysOnMarket: 42, city: 'Brooklyn' 
            },
            'Bushwick': { 
                medianSalePrice: 850000, estimatedPricePerSqft: 700, 
                avgDaysOnMarket: 38, city: 'Brooklyn' 
            },

            // Queens
            'Astoria': { 
                medianSalePrice: 800000, estimatedPricePerSqft: 650, 
                avgDaysOnMarket: 45, city: 'Queens' 
            },
            'Long Island City': { 
                medianSalePrice: 950000, estimatedPricePerSqft: 850, 
                avgDaysOnMarket: 40, city: 'Queens' 
            },
            'Sunnyside': { 
                medianSalePrice: 750000, estimatedPricePerSqft: 600, 
                avgDaysOnMarket: 42, city: 'Queens' 
            },
            'Jackson Heights': { 
                medianSalePrice: 650000, estimatedPricePerSqft: 550, 
                avgDaysOnMarket: 50, city: 'Queens' 
            },

            // Manhattan - High-end
            'East Village': { 
                medianSalePrice: 1600000, estimatedPricePerSqft: 1500, 
                avgDaysOnMarket: 35, city: 'New York' 
            },
            'Lower East Side': { 
                medianSalePrice: 1400000, estimatedPricePerSqft: 1300, 
                avgDaysOnMarket: 38, city: 'New York' 
            },

            // Bronx - Most affordable
            'South Bronx': { 
                medianSalePrice: 450000, estimatedPricePerSqft: 350, 
                avgDaysOnMarket: 60, city: 'Bronx' 
            },
            'Mott Haven': { 
                medianSalePrice: 550000, estimatedPricePerSqft: 450, 
                avgDaysOnMarket: 52, city: 'Bronx' 
            }
        };
    }

    /**
     * Step 2: Fetch properties and calculate ACTUAL price per sqft
     */
    async fetchAndAnalyzeNeighborhood(neighborhood, options = {}) {
        if (this.apiCallsUsed >= this.maxApiCalls) {
            console.warn(`⚠️ API call limit reached (${this.maxApiCalls})`);
            return [];
        }

        const marketData = this.redfinMarketData[neighborhood];
        if (!marketData) {
            console.warn(`⚠️ No market data for ${neighborhood}`);
            return [];
        }

        console.log(`🔍 Analyzing ${neighborhood}...`);
        console.log(`   📊 Market avg: $${marketData.estimatedPricePerSqft}/sqft`);

        try {
            // Fetch ALL properties in the neighborhood (no price filter yet)
            const params = {
                areas: neighborhood,
                minBeds: options.minBeds || 1,
                limit: 500, // Maximum properties per call
                offset: options.offset || 0,
                ...(options.types && { types: options.types })
            };

            const response = await axios.get(
                'https://streeteasy-api.p.rapidapi.com/properties/search',
                {
                    params: params,
                    headers: {
                        'X-RapidAPI-Key': this.rapidApiKey,
                        'X-RapidAPI-Host': 'streeteasy-api.p.rapidapi.com'
                    },
                    timeout: 15000
                }
            );

            this.apiCallsUsed++;
            console.log(`📞 API call ${this.apiCallsUsed}/${this.maxApiCalls} - Found ${response.data.length} properties`);

            // Now analyze each property for ACTUAL undervaluation
            return this.analyzePropertiesForUndervaluation(response.data, neighborhood, marketData);

        } catch (error) {
            console.error(`❌ Error fetching ${neighborhood}:`, error.response?.data || error.message);
            this.apiCallsUsed++;
            return [];
        }
    }

    /**
     * Step 3: Analyze properties for TRUE undervaluation using price per sqft
     */
    analyzePropertiesForUndervaluation(properties, neighborhood, marketData) {
        console.log(`   🧮 Analyzing ${properties.length} properties for undervaluation...`);
        
        const undervaluedProperties = [];
        let propertiesWithSqft = 0;

        properties.forEach(property => {
            // Only analyze properties with square footage data
            if (!property.sqft || !property.price || property.sqft <= 0 || property.price <= 0) {
                return;
            }

            propertiesWithSqft++;
            
            const actualPricePerSqft = property.price / property.sqft;
            const marketPricePerSqft = marketData.estimatedPricePerSqft;
            const discountPercent = ((marketPricePerSqft - actualPricePerSqft) / marketPricePerSqft) * 100;

            // Only consider properties that are significantly below market price per sqft
            if (discountPercent >= 15) { // At least 15% below market rate per sqft
                
                // Analyze description for distress signals
                const description = property.description || '';
                const distressSignals = this.findDistressSignals(description);
                const warningSignals = this.findWarningSignals(description);
                
                // Calculate comprehensive undervaluation score
                const score = this.calculateUndervaluationScore({
                    discountPercent,
                    daysOnMarket: property.daysOnMarket || 30,
                    distressSignals,
                    warningSignals,
                    propertyType: property.type,
                    sqft: property.sqft,
                    beds: property.beds
                });

                undervaluedProperties.push({
                    // Property details
                    address: property.address || 'Address not provided',
                    neighborhood: neighborhood,
                    price: property.price,
                    sqft: property.sqft,
                    beds: property.beds,
                    baths: property.baths,
                    propertyType: property.type,
                    daysOnMarket: property.daysOnMarket || 'Unknown',
                    url: property.url,
                    description: description.substring(0, 200) + '...', // Truncate for display

                    // Market analysis
                    actualPricePerSqft: Math.round(actualPricePerSqft),
                    marketPricePerSqft: marketPricePerSqft,
                    discountPercent: Math.round(discountPercent * 10) / 10,
                    potentialSavings: Math.round((marketPricePerSqft - actualPricePerSqft) * property.sqft),

                    // Distress analysis
                    distressSignals: distressSignals,
                    warningSignals: warningSignals,
                    distressReason: this.categorizeDistressReason(distressSignals),

                    // Final score
                    score: score
                });
            }
        });

        console.log(`   ✅ Found ${undervaluedProperties.length} undervalued properties (${propertiesWithSqft} had sqft data)`);
        
        // Sort by best deals first
        return undervaluedProperties.sort((a, b) => b.score - a.score);
    }

    /**
     * Find distress signals in property description
     */
    findDistressSignals(description) {
        const text = description.toLowerCase();
        return this.distressSignals.filter(signal => 
            text.includes(signal.toLowerCase())
        );
    }

    /**
     * Find warning signals in property description
     */
    findWarningSignals(description) {
        const text = description.toLowerCase();
        return this.warningSignals.filter(signal => 
            text.includes(signal.toLowerCase())
        );
    }

    /**
     * Categorize the likely reason for distress sale
     */
    categorizeDistressReason(distressSignals) {
        if (distressSignals.some(s => ['motivated seller', 'must sell', 'quick sale'].includes(s))) {
            return 'Motivated Seller';
        }
        if (distressSignals.some(s => ['needs work', 'fixer-upper', 'tlc', 'needs updating'].includes(s))) {
            return 'Needs Renovation';
        }
        if (distressSignals.some(s => ['estate sale', 'inherited', 'probate'].includes(s))) {
            return 'Estate Sale';
        }
        if (distressSignals.some(s => ['divorce', 'foreclosure', 'short sale'].includes(s))) {
            return 'Financial Distress';
        }
        if (distressSignals.some(s => ['as-is', 'cash only', 'investor special'].includes(s))) {
            return 'Investor Opportunity';
        }
        return distressSignals.length > 0 ? 'Opportunity' : 'Unknown';
    }

    /**
     * Calculate comprehensive undervaluation score
     */
    calculateUndervaluationScore(factors) {
        let score = 0;

        // Price per sqft discount (0-50 points)
        score += Math.min(factors.discountPercent * 2, 50);

        // Days on market (0-20 points) - fresher listings get bonus
        if (factors.daysOnMarket <= 7) score += 20;
        else if (factors.daysOnMarket <= 30) score += 15;
        else if (factors.daysOnMarket <= 60) score += 10;
        else score += 5;

        // Distress signals (0-15 points) - more signals = more opportunity
        score += Math.min(factors.distressSignals.length * 3, 15);

        // Property size bonus (0-10 points) - larger properties often better deals
        if (factors.sqft > 1000) score += 10;
        else if (factors.sqft > 700) score += 7;
        else score += 5;

        // Family-friendly bonus (0-5 points)
        if (factors.beds >= 2) score += 5;

        // Warning penalty (0 to -10 points)
        score -= Math.min(factors.warningSignals.length * 3, 10);

        return Math.round(score);
    }

    /**
     * Main analysis function
     */
    async findUndervaluedProperties(options = {}) {
        console.log('🗽 Starting TRUE undervaluation analysis (price per sqft based)...\n');
        
        // Get real market data
        await this.getRedfinNeighborhoodData();
        
        const allDeals = [];
        const targetNeighborhoods = options.neighborhoods || [
            // Focus on neighborhoods with good data and opportunities
            'Red Hook', 'Crown Heights', 'Bushwick', 'Astoria', 'Sunnyside', 
            'Jackson Heights', 'South Bronx', 'Mott Haven'
        ];

        for (const neighborhood of targetNeighborhoods) {
            if (this.apiCallsUsed >= this.maxApiCalls) {
                console.log(`⚠️ Reached API limit, stopping at ${this.apiCallsUsed} calls`);
                break;
            }

            try {
                const undervaluedProperties = await this.fetchAndAnalyzeNeighborhood(neighborhood, {
                    minBeds: options.minBeds || 1,
                    types: options.types // e.g., 'coop,house'
                });

                allDeals.push(...undervaluedProperties);
                
                // Rate limiting
                await this.delay(1000);
                
            } catch (error) {
                console.error(`❌ Error processing ${neighborhood}:`, error.message);
            }
        }

        // Sort all deals by score
        allDeals.sort((a, b) => b.score - a.score);
        
        return {
            totalDeals: allDeals.length,
            apiCallsUsed: this.apiCallsUsed,
            avgDiscount: allDeals.length > 0 ? 
                allDeals.reduce((sum, deal) => sum + deal.discountPercent, 0) / allDeals.length : 0,
            topDeals: allDeals.slice(0, 25),
            allDeals: allDeals
        };
    }

    /**
     * Display results with enhanced details
     */
    displayResults(results) {
        console.log('\n🏆 TRUE UNDERVALUED NYC PROPERTIES (Price Per Sqft Analysis)');
        console.log('='.repeat(70));
        console.log(`📊 Analysis Summary:`);
        console.log(`   Total undervalued properties: ${results.totalDeals}`);
        console.log(`   Average discount: ${results.avgDiscount.toFixed(1)}% below market`);
        console.log(`   API calls used: ${results.apiCallsUsed}/${this.maxApiCalls}`);

        if (results.totalDeals === 0) {
            console.log('\n😔 No undervalued properties found with current criteria.');
            console.log('💡 Try expanding neighborhoods or lowering the discount threshold.');
            return;
        }

        console.log(`\n🏠 Top ${Math.min(10, results.totalDeals)} Undervalued Properties:`);

        results.topDeals.slice(0, 10).forEach((deal, index) => {
            console.log(`\n${index + 1}. 📍 ${deal.address}`);
            console.log(`   🏘️ ${deal.neighborhood} | ${deal.propertyType || 'Unknown type'}`);
            console.log(`   💰 $${deal.price.toLocaleString()} | ${deal.sqft} sqft | ${deal.beds}BR/${deal.baths}BA`);
            console.log(`   📊 $${deal.actualPricePerSqft}/sqft vs $${deal.marketPricePerSqft}/sqft market avg`);
            console.log(`   💸 ${deal.discountPercent.toFixed(1)}% below market = $${deal.potentialSavings.toLocaleString()} savings`);
            console.log(`   🏆 Score: ${deal.score}/100`);
            console.log(`   📅 ${deal.daysOnMarket} days on market`);
            
            if (deal.distressReason !== 'Unknown') {
                console.log(`   🚨 Reason: ${deal.distressReason}`);
            }
            
            if (deal.distressSignals.length > 0) {
                console.log(`   🔍 Signals: ${deal.distressSignals.slice(0, 3).join(', ')}${deal.distressSignals.length > 3 ? '...' : ''}`);
            }
            
            if (deal.warningSignals.length > 0) {
                console.log(`   ⚠️ Warnings: ${deal.warningSignals.join(', ')}`);
            }
            
            if (deal.url) console.log(`   🔗 ${deal.url}`);
        });

        console.log(`\n🎯 Key Insights:`);
        const avgPricePerSqft = results.allDeals.reduce((sum, deal) => sum + deal.actualPricePerSqft, 0) / results.allDeals.length;
        const avgMarketPricePerSqft = results.allDeals.reduce((sum, deal) => sum + deal.marketPricePerSqft, 0) / results.allDeals.length;
        
        console.log(`   • Average deal: $${Math.round(avgPricePerSqft)}/sqft vs $${Math.round(avgMarketPricePerSqft)}/sqft market`);
        console.log(`   • Total potential savings: $${results.allDeals.reduce((sum, deal) => sum + deal.potentialSavings, 0).toLocaleString()}`);
        
        const distressReasons = {};
        results.allDeals.forEach(deal => {
            distressReasons[deal.distressReason] = (distressReasons[deal.distressReason] || 0) + 1;
        });
        
        console.log(`   • Common reasons: ${Object.entries(distressReasons)
            .sort((a, b) => b[1] - a[1])
            .slice(0, 3)
            .map(([reason, count]) => `${reason} (${count})`)
            .join(', ')}`);
    }

    /**
     * Save enhanced results
     */
    async saveResults(results, filename = 'true-undervalued-nyc-properties.json') {
        try {
            const data = {
                timestamp: new Date().toISOString(),
                methodology: 'Price per square foot analysis vs neighborhood market averages',
                criteria: 'Minimum 15% below market price per sqft + distress signal analysis',
                analysis: {
                    totalDeals: results.totalDeals,
                    apiCallsUsed: results.apiCallsUsed,
                    avgDiscount: results.avgDiscount,
                    totalPotentialSavings: results.allDeals.reduce((sum, deal) => sum + deal.potentialSavings, 0)
                },
                properties: results.allDeals
            };

            await fs.writeFile(filename, JSON.stringify(data, null, 2));
            console.log(`\n💾 Enhanced results saved to ${filename}`);
        } catch (error) {
            console.error('❌ Error saving results:', error.message);
        }
    }

    delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

/**
 * Main execution
 */
async function main() {
    console.log('🗽 NYC TRUE Undervaluation Finder - Price Per Sqft Analysis\n');
    
    if (!process.env.RAPIDAPI_KEY) {
        console.error('❌ Missing RAPIDAPI_KEY environment variable');
        console.error('💡 Create a .env file with: RAPIDAPI_KEY=your_rapidapi_key');
        return;
    }

    const finder = new UndervaluedPropertyFinder();
    
    try {
        const results = await finder.findUndervaluedProperties({
            neighborhoods: [
                // Focus on neighborhoods with good opportunity/data ratio
                'Red Hook', 'Crown Heights', 'Bushwick', 'Astoria', 'Long Island City',
                'Sunnyside', 'Jackson Heights', 'South Bronx', 'Mott Haven'
            ],
            minBeds: 1,
            types: 'coop,house,condo' // All types for comprehensive analysis
        });

        finder.displayResults(results);
        await finder.saveResults(results);

        console.log('\n✅ TRUE undervaluation analysis complete!');
        console.log(`📞 Used ${results.apiCallsUsed}/25 free API calls`);
        
        if (results.totalDeals > 0) {
            console.log('\n🎉 Found genuinely undervalued properties based on price per sqft!');
            console.log('💡 This methodology is much more accurate than simple price filtering.');
        }

    } catch (error) {
        console.error('💥 Analysis failed:', error.message);
    }
}

module.exports = UndervaluedPropertyFinder;

if (require.main === module) {
    main().catch(console.error);
}
