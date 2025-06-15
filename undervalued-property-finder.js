const RedfinAPIScraper = require('./redfin-scraper.js');

class UndervaluedPropertyFinder {
    constructor() {
        this.scraper = new RedfinAPIScraper();
        
        // Market analysis cache to avoid recalculating
        this.marketCache = new Map();
        this.cacheExpiry = 24 * 60 * 60 * 1000; // 24 hours
        
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

        // NYC boroughs for targeted searching
        this.NYC_BOROUGHS = [
            'Manhattan, NY',
            'Brooklyn, NY',
            'Queens, NY',
            'Bronx, NY',
            'Staten Island, NY'
        ];
    }

    /**
     * Main function to find undervalued properties in NYC
     */
    async findUndervaluedProperties(location, options = {}) {
        console.log(`🗽 Searching for undervalued properties in ${location}...`);
        
        // NYC-optimized default settings
        const settings = {
            minDiscountPercent: options.minDiscountPercent || 15, // At least 15% below market
            maxDaysOnMarket: options.maxDaysOnMarket || 90,       // Fresh listings
            minBeds: options.minBeds || 1,
            maxPrice: options.maxPrice || 3000000,               // NYC pricing
            limit: options.limit || 350,
            ...options
        };

        try {
            // Step 1: Get all active listings in the NYC area
            console.log('📊 Step 1: Fetching all active NYC listings...');
            const allListings = await this.scraper.searchProperties(location, {
                status: 9, // Active only
                limit: settings.limit,
                maxPrice: settings.maxPrice,
                minBeds: settings.minBeds
            });

            if (!allListings.listings || allListings.listings.length === 0) {
                throw new Error('No listings found in this NYC location');
            }

            console.log(`✅ Found ${allListings.listings.length} active listings in ${location}`);

            // Step 2: Calculate NYC market averages by neighborhood/zip/bedroom count
            console.log('📈 Step 2: Calculating NYC market comparables...');
            const marketData = await this.calculateMarketComparables(allListings.listings);

            // Step 3: Analyze each property for undervaluation
            console.log('🎯 Step 3: Analyzing NYC properties for undervaluation...');
            const analyzedProperties = await this.analyzeProperties(allListings.listings, marketData);

            // Step 4: Filter and score undervalued properties
            console.log('🏆 Step 4: Scoring and filtering undervalued NYC properties...');
            const undervaluedProperties = this.filterAndScoreProperties(
                analyzedProperties, 
                settings.minDiscountPercent,
                settings.maxDaysOnMarket
            );

            // Step 5: Sort by score (best deals first)
            undervaluedProperties.sort((a, b) => b.score - a.score);

            console.log(`🎉 Found ${undervaluedProperties.length} potentially undervalued properties in ${location}!`);

            return {
                location: location,
                timestamp: new Date().toISOString(),
                settings: settings,
                marketData: marketData,
                totalListings: allListings.listings.length,
                undervaluedCount: undervaluedProperties.length,
                undervaluedProperties: undervaluedProperties
            };

        } catch (error) {
            console.error('❌ Error finding undervalued NYC properties:', error.message);
            throw error;
        }
    }

    /**
     * Scan all NYC boroughs for undervalued properties
     */
    async scanAllNYCBoroughs(options = {}) {
        console.log('🗽 Scanning ALL NYC boroughs for undervalued properties...\n');
        
        const allResults = {
            timestamp: new Date().toISOString(),
            totalBoroughs: this.NYC_BOROUGHS.length,
            boroughResults: {},
            summary: {
                totalListings: 0,
                totalUndervalued: 0,
                bestDeals: []
            }
        };

        for (const borough of this.NYC_BOROUGHS) {
            try {
                console.log(`\n📍 Processing ${borough}...`);
                
                const results = await this.findUndervaluedProperties(borough, options);
                allResults.boroughResults[borough] = results;
                
                // Update summary
                allResults.summary.totalListings += results.totalListings;
                allResults.summary.totalUndervalued += results.undervaluedCount;
                
                // Collect top 3 deals from each borough
                const topDeals = results.undervaluedProperties.slice(0, 3).map(prop => ({
                    ...prop,
                    borough: borough
                }));
                allResults.summary.bestDeals.push(...topDeals);
                
                console.log(`✅ ${borough}: ${results.undervaluedCount} undervalued properties found`);
                
                // Rate limiting between boroughs
                await new Promise(resolve => setTimeout(resolve, 3000));
                
            } catch (error) {
                console.error(`❌ Error processing ${borough}:`, error.message);
                allResults.boroughResults[borough] = { error: error.message };
            }
        }

        // Sort best deals across all boroughs
        allResults.summary.bestDeals.sort((a, b) => b.score - a.score);
        allResults.summary.bestDeals = allResults.summary.bestDeals.slice(0, 20); // Top 20 citywide

        console.log('\n🗽 NYC BOROUGH SCAN COMPLETE');
        console.log('='.repeat(50));
        console.log(`📊 Total listings analyzed: ${allResults.summary.totalListings}`);
        console.log(`🎯 Total undervalued properties: ${allResults.summary.totalUndervalued}`);
        console.log(`🏆 Top deals collected: ${allResults.summary.bestDeals.length}`);

        return allResults;
    }

    /**
     * Calculate market comparables for NYC pricing analysis
     */
    async calculateMarketComparables(listings) {
        console.log('📊 Calculating NYC market averages...');
        
        const marketData = {
            overall: {},
            byZip: {},
            byBedrooms: {},
            byZipAndBeds: {}
        };

        // Filter out properties with invalid data
        const validListings = listings.filter(listing => {
            const price = this.parsePrice(listing.price);
            const sqft = this.parseSqft(listing.square_feet);
            return price > 0 && sqft > 0;
        });

        if (validListings.length === 0) {
            throw new Error('No valid listings with price and square footage data');
        }

        // Overall market averages
        marketData.overall = this.calculateAverages(validListings);

        // Group by ZIP code (NYC neighborhoods)
        const zipGroups = this.groupBy(validListings, 'zip_or_postal_code');
        for (const [zip, properties] of Object.entries(zipGroups)) {
            if (properties.length >= 3) { // Need at least 3 comps
                marketData.byZip[zip] = this.calculateAverages(properties);
            }
        }

        // Group by bedroom count
        const bedGroups = this.groupBy(validListings, 'beds');
        for (const [beds, properties] of Object.entries(bedGroups)) {
            if (properties.length >= 3) {
                marketData.byBedrooms[beds] = this.calculateAverages(properties);
            }
        }

        // Group by ZIP + Bedrooms (most specific for NYC)
        validListings.forEach(listing => {
            const key = `${listing.zip_or_postal_code}-${listing.beds}`;
            if (!marketData.byZipAndBeds[key]) {
                marketData.byZipAndBeds[key] = [];
            }
            marketData.byZipAndBeds[key].push(listing);
        });

        // Calculate averages for ZIP + bedroom combinations
        for (const [key, properties] of Object.entries(marketData.byZipAndBeds)) {
            if (properties.length >= 2) { // Need at least 2 comps for specific combo
                marketData.byZipAndBeds[key] = this.calculateAverages(properties);
            } else {
                delete marketData.byZipAndBeds[key]; // Remove insufficient data
            }
        }

        console.log(`✅ NYC market analysis complete:`);
        console.log(`   - Overall: ${validListings.length} properties`);
        console.log(`   - ZIP codes: ${Object.keys(marketData.byZip).length}`);
        console.log(`   - Bedroom groups: ${Object.keys(marketData.byBedrooms).length}`);
        console.log(`   - ZIP+Bedroom combos: ${Object.keys(marketData.byZipAndBeds).length}`);

        return marketData;
    }

    /**
     * Calculate price averages for a group of properties
     */
    calculateAverages(properties) {
        const prices = properties.map(p => this.parsePrice(p.price)).filter(p => p > 0);
        const sqfts = properties.map(p => this.parseSqft(p.square_feet)).filter(s => s > 0);
        const pricesPerSqft = properties.map(p => {
            const price = this.parsePrice(p.price);
            const sqft = this.parseSqft(p.square_feet);
            return (price > 0 && sqft > 0) ? price / sqft : 0;
        }).filter(psf => psf > 0);

        if (prices.length === 0) return null;

        const sortedPrices = [...prices].sort((a, b) => a - b);
        const sortedPsf = [...pricesPerSqft].sort((a, b) => a - b);

        return {
            count: properties.length,
            avgPrice: prices.reduce((a, b) => a + b, 0) / prices.length,
            medianPrice: sortedPrices[Math.floor(sortedPrices.length / 2)],
            avgPricePerSqft: pricesPerSqft.length > 0 ? pricesPerSqft.reduce((a, b) => a + b, 0) / pricesPerSqft.length : 0,
            medianPricePerSqft: sortedPsf.length > 0 ? sortedPsf[Math.floor(sortedPsf.length / 2)] : 0,
            minPrice: Math.min(...prices),
            maxPrice: Math.max(...prices)
        };
    }

    /**
     * Analyze each property for undervaluation
     */
    async analyzeProperties(listings, marketData) {
        console.log('🔍 Analyzing individual NYC properties...');
        
        const analyzedProperties = [];

        for (const listing of listings) {
            try {
                const analysis = await this.analyzeProperty(listing, marketData);
                if (analysis) {
                    analyzedProperties.push(analysis);
                }
            } catch (error) {
                console.warn(`⚠️ Could not analyze NYC property ${listing.address}: ${error.message}`);
            }
        }

        return analyzedProperties;
    }

    /**
     * Analyze a single property against NYC market comparables
     */
    async analyzeProperty(listing, marketData) {
        const price = this.parsePrice(listing.price);
        const sqft = this.parseSqft(listing.square_feet);
        
        if (price <= 0 || sqft <= 0) {
            return null; // Skip properties without valid price/sqft
        }

        const pricePerSqft = price / sqft;
        const zip = listing.zip_or_postal_code;
        const beds = listing.beds;
        const daysOnMarket = parseInt(listing.days_on_market) || 0;

        // Get most specific comparable data available
        let comparableData = null;
        let comparisonLevel = '';

        // Try ZIP + Bedrooms first (most specific for NYC neighborhoods)
        const zipBedKey = `${zip}-${beds}`;
        if (marketData.byZipAndBeds[zipBedKey]) {
            comparableData = marketData.byZipAndBeds[zipBedKey];
            comparisonLevel = 'NYC Neighborhood + Bedrooms';
        }
        // Fall back to ZIP only (NYC neighborhood)
        else if (marketData.byZip[zip]) {
            comparableData = marketData.byZip[zip];
            comparisonLevel = 'NYC Neighborhood';
        }
        // Fall back to bedroom count
        else if (marketData.byBedrooms[beds]) {
            comparableData = marketData.byBedrooms[beds];
            comparisonLevel = 'Bedroom Count';
        }
        // Last resort: overall market
        else {
            comparableData = marketData.overall;
            comparisonLevel = 'Overall NYC Market';
        }

        if (!comparableData) {
            return null;
        }

        // Calculate how much below market this property is
        const marketPricePerSqft = comparableData.medianPricePerSqft;
        const expectedPrice = marketPricePerSqft * sqft;
        const priceDifference = expectedPrice - price;
        const percentBelowMarket = (priceDifference / expectedPrice) * 100;

        // Analyze description for distress signals
        const description = listing.description || '';
        const distressSignals = this.findDistressSignals(description);
        const warningTags = this.findWarningSignals(description);

        // Get property details if available
        let detailedAnalysis = null;
        if (listing.url) {
            try {
                const details = await this.scraper.getPropertyDetails(listing.url);
                detailedAnalysis = this.analyzePropertyDetails(details);
            } catch (error) {
                // Details not available, continue with basic analysis
            }
        }

        return {
            // Basic property info
            address: listing.address,
            city: listing.city,
            zip: zip,
            price: price,
            beds: beds,
            baths: listing.baths,
            sqft: sqft,
            pricePerSqft: pricePerSqft,
            daysOnMarket: daysOnMarket,
            url: listing.url,
            description: description,

            // Market analysis
            marketPricePerSqft: marketPricePerSqft,
            expectedPrice: expectedPrice,
            priceDifference: priceDifference,
            percentBelowMarket: percentBelowMarket,
            comparisonLevel: comparisonLevel,
            comparableCount: comparableData.count,

            // Distress and warning signals
            distressSignals: distressSignals,
            warningTags: warningTags,
            
            // Additional analysis
            detailedAnalysis: detailedAnalysis,
            
            // Will be calculated in scoring phase
            score: 0,
            reasoning: ''
        };
    }

    /**
     * Filter properties that are undervalued and calculate scores
     */
    filterAndScoreProperties(analyzedProperties, minDiscountPercent, maxDaysOnMarket) {
        console.log(`🎯 Filtering for NYC properties ${minDiscountPercent}%+ below market...`);

        const undervalued = analyzedProperties.filter(property => {
            return property.percentBelowMarket >= minDiscountPercent &&
                   property.daysOnMarket <= maxDaysOnMarket;
        });

        console.log(`✅ ${undervalued.length} NYC properties meet undervaluation criteria`);

        // Calculate scores for each undervalued property
        return undervalued.map(property => {
            const scored = this.calculatePropertyScore(property);
            return scored;
        });
    }

    /**
     * Calculate a comprehensive score for how good of a deal a NYC property is
     */
    calculatePropertyScore(property) {
        let score = 0;
        let reasoning = [];

        // Base score from percentage below market (0-50 points)
        const percentScore = Math.min(property.percentBelowMarket * 2, 50);
        score += percentScore;
        reasoning.push(`${property.percentBelowMarket.toFixed(1)}% below NYC market (+${percentScore.toFixed(1)} pts)`);

        // Days on market scoring (0-20 points) - newer is better in NYC
        let domScore = 0;
        if (property.daysOnMarket <= 3) {
            domScore = 20;
            reasoning.push(`Very fresh NYC listing (${property.daysOnMarket} days) (+20 pts)`);
        } else if (property.daysOnMarket <= 7) {
            domScore = 15;
            reasoning.push(`Fresh NYC listing (${property.daysOnMarket} days) (+15 pts)`);
        } else if (property.daysOnMarket <= 30) {
            domScore = 10;
            reasoning.push(`Recent NYC listing (${property.daysOnMarket} days) (+10 pts)`);
        } else {
            domScore = 5;
            reasoning.push(`Older NYC listing (${property.daysOnMarket} days) (+5 pts)`);
        }
        score += domScore;

        // Distress signals (0-15 points) - more signals = better deal potential
        const distressScore = Math.min(property.distressSignals.length * 3, 15);
        if (distressScore > 0) {
            score += distressScore;
            reasoning.push(`${property.distressSignals.length} distress signals: ${property.distressSignals.join(', ')} (+${distressScore} pts)`);
        }

        // Comparison quality bonus (0-10 points) - NYC neighborhoods matter a lot
        let compScore = 0;
        if (property.comparisonLevel === 'NYC Neighborhood + Bedrooms') {
            compScore = 10;
            reasoning.push(`High-quality comparison (NYC neighborhood + bedrooms, ${property.comparableCount} comps) (+10 pts)`);
        } else if (property.comparisonLevel === 'NYC Neighborhood') {
            compScore = 8;
            reasoning.push(`Good comparison (NYC neighborhood, ${property.comparableCount} comps) (+8 pts)`);
        } else if (property.comparisonLevel === 'Bedroom Count') {
            compScore = 5;
            reasoning.push(`Fair comparison (bedroom count, ${property.comparableCount} comps) (+5 pts)`);
        } else {
            compScore = 3;
            reasoning.push(`Basic comparison (overall NYC market, ${property.comparableCount} comps) (+3 pts)`);
        }
        score += compScore;

        // NYC price range bonus (0-5 points) - different sweet spots for NYC
        let priceScore = 0;
        if (property.price >= 500000 && property.price <= 1500000) {
            priceScore = 5;
            reasoning.push(`Good NYC price range for investing (+5 pts)`);
        } else if (property.price >= 1500000 && property.price <= 2500000) {
            priceScore = 3;
            reasoning.push(`Higher NYC price range (+3 pts)`);
        } else if (property.price < 500000) {
            priceScore = 2;
            reasoning.push(`Low NYC price range - may have issues (+2 pts)`);
        }
        score += priceScore;

        // Warning signals penalty (0 to -10 points)
        const warningPenalty = Math.min(property.warningTags.length * 2, 10);
        if (warningPenalty > 0) {
            score -= warningPenalty;
            reasoning.push(`${property.warningTags.length} warning signals: ${property.warningTags.join(', ')} (-${warningPenalty} pts)`);
        }

        // Final scoring and grade
        let grade = 'F';
        if (score >= 80) grade = 'A+';
        else if (score >= 70) grade = 'A';
        else if (score >= 60) grade = 'B';
        else if (score >= 50) grade = 'C';
        else if (score >= 40) grade = 'D';

        return {
            ...property,
            score: Math.round(score),
            grade: grade,
            reasoning: reasoning.join('; ')
        };
    }

    /**
     * Find distress signals in NYC property description
     */
    findDistressSignals(description) {
        const text = description.toLowerCase();
        return this.distressKeywords.filter(keyword => 
            text.includes(keyword.toLowerCase())
        );
    }

    /**
     * Find warning signals in NYC property description
     */
    findWarningSignals(description) {
        const text = description.toLowerCase();
        return this.warningKeywords.filter(keyword => 
            text.includes(keyword.toLowerCase())
        );
    }

    /**
     * Analyze detailed property information
     */
    analyzePropertyDetails(details) {
        if (!details || !details.data) return null;

        const analysis = {
            hasPhotos: false,
            photoCount: 0,
            hasVirtualTour: false,
            keyFeatures: []
        };

        // Analyze photos
        if (details.data.images && Array.isArray(details.data.images)) {
            analysis.hasPhotos = details.data.images.length > 0;
            analysis.photoCount = details.data.images.length;
        }

        // Look for key features in detailed data (NYC-specific)
        const detailText = JSON.stringify(details.data).toLowerCase();
        
        const positiveFeatures = [
            'updated kitchen', 'new roof', 'new hvac', 'hardwood floors',
            'granite counters', 'stainless appliances', 'garage', 'basement',
            'fireplace', 'deck', 'patio', 'pool', 'doorman', 'elevator',
            'washer dryer', 'dishwasher', 'central air', 'exposed brick',
            'high ceilings', 'original details', 'crown molding'
        ];

        analysis.keyFeatures = positiveFeatures.filter(feature =>
            detailText.includes(feature)
        );

        return analysis;
    }

    /**
     * Format results for database storage (Supabase format)
     */
    formatForDatabase(results) {
        return results.undervaluedProperties.map(property => ({
            address: property.address,
            price: `${property.price.toLocaleString()}`,
            beds: property.beds?.toString() || null,
            sqft: property.sqft?.toString() || null,
            zip: property.zip,
            link: property.url,
            score: property.score,
            percent_below_market: property.percentBelowMarket,
            warning_tags: property.warningTags
        }));
    }

    /**
     * Helper functions
     */
    parsePrice(priceStr) {
        if (!priceStr) return 0;
        const cleaned = priceStr.toString().replace(/[$,]/g, '');
        const price = parseFloat(cleaned);
        return isNaN(price) ? 0 : price;
    }

    parseSqft(sqftStr) {
        if (!sqftStr) return 0;
        const cleaned = sqftStr.toString().replace(/[,]/g, '');
        const sqft = parseFloat(cleaned);
        return isNaN(sqft) ? 0 : sqft;
    }

    groupBy(array, key) {
        return array.reduce((groups, item) => {
            const value = item[key] || 'unknown';
            if (!groups[value]) groups[value] = [];
            groups[value].push(item);
            return groups;
        }, {});
    }

    /**
     * Save results to file
     */
    async saveResults(results, filename) {
        await this.scraper.saveToFile(results, filename);
        
        // Also save database-formatted version
        const dbFormat = this.formatForDatabase(results);
        await this.scraper.saveToFile(dbFormat, filename.replace('.json', '-db.json'));
    }
}

// NYC-focused example usage
async function findNYCUndervaluedExample() {
    console.log('🗽 NYC Undervalued Property Finder Example\n');
    
    const finder = new UndervaluedPropertyFinder();
    
    try {
        // Find undervalued properties in Manhattan
        const manhattanResults = await finder.findUndervaluedProperties('Manhattan, NY', {
            minDiscountPercent: 15,  // At least 15% below market
            maxDaysOnMarket: 60,     // Listed within 60 days
            maxPrice: 2500000,       // Under $2.5M for Manhattan
            limit: 200               // Check 200 listings
        });

        console.log('\n' + '='.repeat(60));
        console.log('🗽 NYC UNDERVALUED PROPERTIES FOUND');
        console.log('='.repeat(60));

        if (manhattanResults.undervaluedProperties.length === 0) {
            console.log('😔 No undervalued properties found in Manhattan with current criteria');
            
            // Try Brooklyn as backup
            console.log('\n🔄 Trying Brooklyn instead...');
            const brooklynResults = await finder.findUndervaluedProperties('Brooklyn, NY', {
                minDiscountPercent: 15,
                maxDaysOnMarket: 60,
                maxPrice: 1500000,
                limit: 200
            });
            
            if (brooklynResults.undervaluedProperties.length > 0) {
                await displayResults(brooklynResults, finder);
            } else {
                console.log('😔 No undervalued properties found in Brooklyn either');
            }
            return;
        }

        await displayResults(manhattanResults, finder);
        
    } catch (error) {
        console.error('❌ Error finding undervalued NYC properties:', error.message);
    }
}

// Helper function to display results
async function displayResults(results, finder) {
    // Show top 10 deals
    const topDeals = results.undervaluedProperties.slice(0, 10);
    
    topDeals.forEach((property, index) => {
        console.log(`\n${index + 1}. 🏠 ${property.address}`);
        console.log(`   💰 Price: ${property.price.toLocaleString()} (${property.percentBelowMarket.toFixed(1)}% below market)`);
        console.log(`   📊 Expected: ${property.expectedPrice.toLocaleString()}`);
        console.log(`   🏆 Score: ${property.score}/100 (Grade: ${property.grade})`);
        console.log(`   🛏️ ${property.beds} bed, ${property.baths} bath, ${property.sqft.toLocaleString()} sqft`);
        console.log(`   📅 ${property.daysOnMarket} days on market`);
        console.log(`   📍 ZIP: ${property.zip}`);
        
        if (property.distressSignals.length > 0) {
            console.log(`   🚨 Distress signals: ${property.distressSignals.join(', ')}`);
        }
        
        if (property.warningTags.length > 0) {
            console.log(`   ⚠️ Warnings: ${property.warningTags.join(', ')}`);
        }
        
        console.log(`   🧠 Reasoning: ${property.reasoning}`);
        console.log(`   🔗 ${property.url}`);
    });

    // Save results
    await finder.saveResults(results, 'nyc-undervalued-properties.json');
    
    console.log('\n📊 SUMMARY:');
    console.log(`✅ Analyzed ${results.totalListings} total NYC listings`);
    console.log(`🎯 Found ${results.undervaluedCount} undervalued properties`);
    console.log(`💾 Results saved to nyc-undervalued-properties.json and nyc-undervalued-properties-db.json`);
    
    return results;
}

// Function to scan all NYC boroughs
async function scanAllNYCExample() {
    console.log('🗽 Scanning ALL NYC Boroughs for Deals\n');
    
    const finder = new UndervaluedPropertyFinder();
    
    try {
        const allResults = await finder.scanAllNYCBoroughs({
            minDiscountPercent: 15,
            maxDaysOnMarket: 90,
            maxPrice: 2000000,
            limit: 100
        });

        console.log('\n🏆 TOP NYC DEALS ACROSS ALL BOROUGHS:');
        console.log('='.repeat(60));

        allResults.summary.bestDeals.slice(0, 10).forEach((property, index) => {
            console.log(`\n${index + 1}. 🏠 ${property.address} (${property.borough})`);
            console.log(`   💰 ${property.price.toLocaleString()} (${property.percentBelowMarket.toFixed(1)}% below market)`);
            console.log(`   🏆 Score: ${property.score}/100`);
        });

        // Save all results
        await finder.saveResults({ undervaluedProperties: allResults.summary.bestDeals }, 'nyc-all-boroughs-deals.json');
        
        console.log(`\n📊 CITYWIDE SUMMARY:`);
        console.log(`🗽 Boroughs scanned: ${allResults.totalBoroughs}`);
        console.log(`📋 Total listings: ${allResults.summary.totalListings}`);
        console.log(`🎯 Total undervalued: ${allResults.summary.totalUndervalued}`);
        console.log(`💾 Results saved to nyc-all-boroughs-deals.json`);

    } catch (error) {
        console.error('❌ Error scanning NYC boroughs:', error.message);
    }
}

// Run if executed directly
if (require.main === module) {
    const args = process.argv.slice(2);
    
    if (args.includes('--all-boroughs')) {
        scanAllNYCExample().catch(console.error);
    } else {
        findNYCUndervaluedExample().catch(console.error);
    }
}

module.exports = UndervaluedPropertyFinder;
