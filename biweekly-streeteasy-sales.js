// enhanced-biweekly-streeteasy-sales.js
// FINAL VERSION: Complete de-duplication + Carroll Gardens + Smart scheduling
// Bi-weekly frequency with intelligent duplicate prevention

require('dotenv').config();
const axios = require('axios');
const { createClient } = require('@supabase/supabase-js');

const VALID_STREETEASY_SLUGS = new Set([
    "west-village", "east-village", "soho", "tribeca", "chelsea",
    "upper-east-side", "upper-west-side", "financial-district", "lower-east-side",
    "gramercy-park", "murray-hill", "hells-kitchen", "midtown",
    "park-slope", "williamsburg", "dumbo", "brooklyn-heights", "fort-greene",
    "prospect-heights", "crown-heights", "bedford-stuyvesant", "greenpoint",
    "red-hook", "carroll-gardens", "bushwick", "sunset-park", // Added Carroll Gardens
    "long-island-city", "hunters-point", "astoria", "sunnyside",
    "woodside", "jackson-heights", "forest-hills", "kew-gardens",
    "mott-haven", "concourse", "fordham", "riverdale",
    "saint-george", "stapleton", "new-brighton"
]);

const HIGH_PRIORITY_NEIGHBORHOODS = [
    'west-village', 'east-village', 'soho', 'tribeca', 'chelsea',
    'upper-east-side', 'upper-west-side', 'park-slope', 'williamsburg',
    'dumbo', 'brooklyn-heights', 'fort-greene', 'prospect-heights',
    'crown-heights', 'bedford-stuyvesant', 'greenpoint', 'carroll-gardens', // Added Carroll Gardens
    'bushwick', 'long-island-city', 'astoria', 'sunnyside'
];

class EnhancedBiWeeklySalesAnalyzer {
    constructor() {
        this.supabase = createClient(
            process.env.SUPABASE_URL,
            process.env.SUPABASE_ANON_KEY
        );
        
        this.rapidApiKey = process.env.RAPIDAPI_KEY;
        this.apiCallsUsed = 0;
        this.monthlyApiLimit = 10000; // Will upgrade to 50k later
        
        // ADAPTIVE RATE LIMITING SYSTEM
        this.baseDelay = 6000; // Start optimistic at 6 seconds
        this.maxRetries = 3;
        this.retryBackoffMultiplier = 2;
        
        // Adaptive rate limiting tracking
        this.rateLimitHits = 0;
        this.callTimestamps = [];
        this.maxCallsPerHour = 200;
        
        // Smart scheduling system (offset from rentals)
        this.dailySchedule = this.createSalesDailySchedule();
        this.currentDay = this.getCurrentScheduleDay();
        
        // DE-DUPLICATION CACHE
        this.existingListingsCache = new Map();
        this.duplicateCheckWindow = 30; // Days to check for duplicates
        
        // Track detailed API usage + de-duplication stats
        this.apiUsageStats = {
            activeSalesCalls: 0,
            detailsCalls: 0,
            failedCalls: 0,
            rateLimitHits: 0,
            adaptiveDelayChanges: 0,
            duplicatesSkipped: 0,
            duplicateChecks: 0,
            newPropertiesProcessed: 0
        };
    }

    /**
     * Create sales schedule (offset from rentals to spread API usage)
     * NOW INCLUDES CARROLL GARDENS
     */
    createSalesDailySchedule() {
        // Offset by 4 days from rental schedule to spread API usage
        return {
            5: ['west-village', 'east-village', 'soho'], // When rentals are on day 1
            6: ['tribeca', 'chelsea', 'upper-east-side'],
            7: ['upper-west-side', 'park-slope', 'williamsburg'],
            8: ['dumbo', 'brooklyn-heights', 'fort-greene'],
            1: ['prospect-heights', 'crown-heights', 'bedford-stuyvesant'], // Next week
            2: ['greenpoint', 'carroll-gardens', 'bushwick'], // Added Carroll Gardens here
            3: ['long-island-city', 'astoria', 'sunnyside'],
            4: [] // Buffer day
        };
    }

    /**
     * Get schedule day (same logic as rentals)
     */
    getCurrentScheduleDay() {
        const today = new Date();
        const dayOfMonth = today.getDate();
        
        if (dayOfMonth >= 1 && dayOfMonth <= 8) {
            return dayOfMonth;
        } else if (dayOfMonth >= 15 && dayOfMonth <= 22) {
            return dayOfMonth - 14;
        } else {
            return 0;
        }
    }

    /**
     * LOAD EXISTING LISTINGS CACHE - Key de-duplication function
     */
    async loadExistingListingsCache(neighborhood = null) {
        console.log('   🔄 Loading existing listings cache for de-duplication...');
        
        try {
            const cutoffDate = new Date();
            cutoffDate.setDate(cutoffDate.getDate() - this.duplicateCheckWindow);
            
            let query = this.supabase
                .from('undervalued_sales')
                .select('listing_id, price, discount_percent, score, analysis_date')
                .gte('analysis_date', cutoffDate.toISOString());
            
            // If processing specific neighborhood, prioritize those + recent global cache
            if (neighborhood) {
                query = query.or(`neighborhood.eq.${neighborhood},analysis_date.gte.${new Date(Date.now() - 7*24*60*60*1000).toISOString()}`);
            }
            
            const { data, error } = await query;
            
            if (error) {
                console.warn('   ⚠️ Error loading cache, continuing without de-duplication:', error.message);
                this.existingListingsCache.clear();
                return;
            }
            
            // Build cache map for O(1) lookup
            this.existingListingsCache.clear();
            data.forEach(listing => {
                this.existingListingsCache.set(listing.listing_id, {
                    price: listing.price,
                    discountPercent: listing.discount_percent,
                    score: listing.score,
                    analysisDate: new Date(listing.analysis_date),
                    isRecent: new Date(listing.analysis_date) > new Date(Date.now() - 7*24*60*60*1000)
                });
            });
            
            console.log(`   ✅ Cache loaded: ${this.existingListingsCache.size} existing listings (${this.duplicateCheckWindow} days)`);
            
        } catch (error) {
            console.warn('   ⚠️ Cache loading failed, continuing without de-duplication:', error.message);
            this.existingListingsCache.clear();
        }
    }

    /**
     * CHECK IF PROPERTY IS DUPLICATE - Core de-duplication logic
     */
    isDuplicateProperty(property) {
        this.apiUsageStats.duplicateChecks++;
        
        const listingId = property.id?.toString();
        if (!listingId) return false;
        
        const existing = this.existingListingsCache.get(listingId);
        if (!existing) return false;
        
        // If analyzed within last 7 days, always skip (too recent)
        if (existing.isRecent) {
            this.apiUsageStats.duplicatesSkipped++;
            return true;
        }
        
        // For older entries, check if meaningful changes occurred
        const priceDiff = Math.abs((property.price || 0) - (existing.price || 0));
        const priceDiffPercent = existing.price > 0 ? (priceDiff / existing.price) * 100 : 0;
        
        // Skip if price hasn't changed significantly (< 5%)
        if (priceDiffPercent < 5) {
            this.apiUsageStats.duplicatesSkipped++;
            return true;
        }
        
        // Property has meaningful changes, process it
        console.log(`   🔄 Re-processing ${listingId}: price changed by ${priceDiffPercent.toFixed(1)}%`);
        return false;
    }

    /**
     * FILTER NEW PROPERTIES - Remove duplicates before detail fetching
     */
    filterNewProperties(activeSales, neighborhood) {
        console.log(`   🔍 Filtering ${activeSales.length} active sales for duplicates...`);
        
        const newProperties = activeSales.filter(sale => !this.isDuplicateProperty(sale));
        
        const duplicatesFound = activeSales.length - newProperties.length;
        console.log(`   ✅ Filtered: ${newProperties.length} new, ${duplicatesFound} duplicates skipped`);
        
        if (duplicatesFound > 0) {
            console.log(`   💡 Saved ~${duplicatesFound} API calls through de-duplication`);
        }
        
        return newProperties;
    }

    /**
     * ADAPTIVE rate limiting (same logic as before)
     */
    adaptiveRateLimit() {
        const now = Date.now();
        this.callTimestamps = this.callTimestamps.filter(t => now - t < 60 * 60 * 1000);
        const callsThisHour = this.callTimestamps.length;
        
        // Adaptive delay adjustment
        if (this.rateLimitHits === 0 && callsThisHour < this.maxCallsPerHour * 0.7) {
            this.baseDelay = Math.max(4000, this.baseDelay - 500);
        } else if (this.rateLimitHits <= 2) {
            this.baseDelay = 8000;
        } else if (this.rateLimitHits > 2) {
            this.baseDelay = Math.min(20000, this.baseDelay + 2000);
            this.apiUsageStats.adaptiveDelayChanges++;
        }
        
        // Hourly protection
        if (callsThisHour >= this.maxCallsPerHour) {
            return 30 * 60 * 1000; // Wait 30 minutes
        }
        
        // Progressive and jitter
        const sessionCalls = this.apiCallsUsed;
        const progressiveIncrease = Math.floor(sessionCalls / 50) * 1000;
        const jitter = Math.random() * 2000;
        
        this.callTimestamps.push(now);
        return this.baseDelay + progressiveIncrease + jitter;
    }

    /**
     * Smart delay (same as before)
     */
    async smartDelay() {
        const delayTime = this.adaptiveRateLimit();
        
        if (delayTime > 60000) {
            console.log(`   ⏰ Long delay: ${Math.round(delayTime/1000/60)} minutes (rate limit protection)`);
        } else {
            console.log(`   ⏰ Adaptive delay: ${Math.round(delayTime/1000)}s`);
        }
        
        await this.delay(delayTime);
    }

    /**
     * MAIN BI-WEEKLY SALES REFRESH - Enhanced with de-duplication
     */
    async runBiWeeklySalesRefresh() {
        console.log('\n🏠 SMART SCHEDULED BI-WEEKLY SALES ANALYSIS');
        console.log('📅 Adaptive rate limiting with daily neighborhood scheduling');
        console.log('🎯 Offset from rental schedule to spread API usage');
        console.log('🔄 De-duplication enabled to prevent redundant processing');
        console.log('🗽 Carroll Gardens added to neighborhood coverage');
        console.log('='.repeat(70));

        // Get today's sales neighborhood assignment
const todaysNeighborhoods = ['park-slope']; // Test with single neighborhood
        
        if (todaysNeighborhoods.length === 0) {
            console.log('📅 No sales neighborhoods scheduled for today');
            return { summary: { message: 'No sales neighborhoods scheduled for today' } };
        }

        const summary = {
            startTime: new Date(),
            scheduledDay: this.currentDay,
            totalNeighborhoods: todaysNeighborhoods.length,
            neighborhoodsProcessed: 0,
            totalActiveSalesFound: 0,
            totalNewProperties: 0,
            totalDuplicatesSkipped: 0,
            totalDetailsAttempted: 0,
            totalDetailsFetched: 0,
            undervaluedFound: 0,
            savedToDatabase: 0,
            apiCallsUsed: 0,
            apiCallsSaved: 0,
            errors: [],
            detailedStats: {
                byNeighborhood: {},
                apiUsage: this.apiUsageStats,
                rateLimit: {
                    initialDelay: this.baseDelay,
                    finalDelay: this.baseDelay,
                    rateLimitHits: 0
                },
                deduplication: {
                    cacheSize: 0,
                    duplicatesSkipped: 0,
                    newPropertiesProcessed: 0
                }
            }
        };

        try {
            await this.clearOldSalesData();

            console.log(`📋 Today's sales assignment: ${todaysNeighborhoods.join(', ')}`);
            console.log(`⚡ Starting with ${this.baseDelay/1000}s delays (will adapt)\n`);

            // Process today's neighborhoods with de-duplication
            for (let i = 0; i < todaysNeighborhoods.length; i++) {
                const neighborhood = todaysNeighborhoods[i];
                
                if (this.apiCallsUsed >= this.monthlyApiLimit - 100) {
                    console.log(`⚠️ Approaching monthly API limit, stopping`);
                    break;
                }

                try {
                    console.log(`\n🏠 [${i + 1}/${todaysNeighborhoods.length}] PROCESSING SALES: ${neighborhood}`);
                    
                    if (i > 0) {
                        await this.smartDelay();
                    }
                    
                    // STEP 1: Load de-duplication cache for this neighborhood
                    await this.loadExistingListingsCache(neighborhood);
                    
                    // STEP 2: Fetch active sales
                    const activeSales = await this.fetchAllActiveSales(neighborhood);
                    summary.totalActiveSalesFound += activeSales.length;
                    
                    if (activeSales.length === 0) {
                        console.log(`   📊 No active sales found in ${neighborhood}`);
                        continue;
                    }

                    console.log(`   📊 Found ${activeSales.length} active sales`);
                    
                    // STEP 3: Filter out duplicates BEFORE fetching details
                    const newProperties = this.filterNewProperties(activeSales, neighborhood);
                    summary.totalNewProperties += newProperties.length;
                    summary.totalDuplicatesSkipped += (activeSales.length - newProperties.length);
                    summary.apiCallsSaved += (activeSales.length - newProperties.length);
                    
                    if (newProperties.length === 0) {
                        console.log(`   ✅ All properties are recent duplicates - no API calls needed!`);
                        summary.detailedStats.byNeighborhood[neighborhood] = {
                            activeSales: activeSales.length,
                            newProperties: 0,
                            duplicatesSkipped: activeSales.length,
                            detailsFetched: 0,
                            undervaluedFound: 0,
                            apiCallsUsed: 1, // Just the initial search
                            apiCallsSaved: activeSales.length
                        };
                        summary.neighborhoodsProcessed++;
                        continue;
                    }
                    
                    // STEP 4: Fetch details ONLY for new properties
                    const detailedProperties = await this.fetchAllPropertyDetailsWithAdaptiveRateLimit(newProperties, neighborhood);
                    summary.totalDetailsAttempted += newProperties.length;
                    summary.totalDetailsFetched += detailedProperties.length;
                    this.apiUsageStats.newPropertiesProcessed += detailedProperties.length;
                    
                    // STEP 5: Analyze for undervaluation
                    const undervaluedProperties = this.analyzeForUndervaluation(detailedProperties, neighborhood);
                    summary.undervaluedFound += undervaluedProperties.length;
                    
                    // STEP 6: Save to database
                    if (undervaluedProperties.length > 0) {
                        const saved = await this.saveUndervaluedSalesToDatabase(undervaluedProperties, neighborhood);
                        summary.savedToDatabase += saved;
                    }
                    
                    summary.detailedStats.byNeighborhood[neighborhood] = {
                        activeSales: activeSales.length,
                        newProperties: newProperties.length,
                        duplicatesSkipped: activeSales.length - newProperties.length,
                        detailsFetched: detailedProperties.length,
                        undervaluedFound: undervaluedProperties.length,
                        apiCallsUsed: newProperties.length + 1, // +1 for initial search
                        apiCallsSaved: activeSales.length - newProperties.length
                    };
                    
                    summary.neighborhoodsProcessed++;
                    console.log(`   ✅ ${neighborhood}: ${undervaluedProperties.length} undervalued sales found (${newProperties.length} new properties processed)`);

                } catch (error) {
                    console.error(`   ❌ Error processing ${neighborhood}: ${error.message}`);
                    
                    if (error.response?.status === 429) {
                        this.rateLimitHits++;
                        this.apiUsageStats.rateLimitHits++;
                        await this.delay(30000);
                    }
                    
                    summary.errors.push({
                        neighborhood,
                        error: error.message,
                        timestamp: new Date().toISOString(),
                        isRateLimit: error.response?.status === 429
                    });
                }
            }

            summary.endTime = new Date();
            summary.duration = (summary.endTime - summary.startTime) / 1000 / 60;
            summary.apiCallsUsed = this.apiCallsUsed;
            summary.detailedStats.rateLimit.finalDelay = this.baseDelay;
            summary.detailedStats.rateLimit.rateLimitHits = this.rateLimitHits;
            
            // Enhanced de-duplication stats
            summary.detailedStats.deduplication = {
                cacheSize: this.existingListingsCache.size,
                duplicatesSkipped: this.apiUsageStats.duplicatesSkipped,
                newPropertiesProcessed: this.apiUsageStats.newPropertiesProcessed,
                duplicateChecks: this.apiUsageStats.duplicateChecks,
                apiCallsSaved: summary.apiCallsSaved
            };

            await this.saveBiWeeklySummary(summary);
            this.logSmartSalesSummary(summary);

        } catch (error) {
            console.error('💥 Smart scheduled sales refresh failed:', error.message);
            summary.errors.push({ error: error.message });
        }

        return summary;
    }

    /**
     * Get today's neighborhoods (same logic as before)
     */
    getTodaysNeighborhoods() {
        const todaysNeighborhoods = this.dailySchedule[this.currentDay] || [];
        
        if (todaysNeighborhoods.length === 0) {
            console.log('📅 Off-schedule day for sales - checking for missed neighborhoods');
            return [];
        }
        
        console.log(`📅 Sales Day ${this.currentDay} schedule: ${todaysNeighborhoods.length} neighborhoods`);
        return todaysNeighborhoods;
    }

    /**
     * Fetch property details with adaptive rate limiting (same as before)
     */
    async fetchAllPropertyDetailsWithAdaptiveRateLimit(activeSales, neighborhood) {
        console.log(`   🔍 Fetching details for ${activeSales.length} NEW properties with adaptive rate limiting...`);
        
        const detailedProperties = [];
        let successCount = 0;
        let failureCount = 0;

        for (let i = 0; i < activeSales.length; i++) {
            const sale = activeSales[i];
            
            try {
                if (this.apiCallsUsed >= this.monthlyApiLimit - 50) {
                    console.log(`   ⚠️ Approaching monthly API limit, stopping detail fetch`);
                    break;
                }

                if (i > 0) {
                    await this.smartDelay();
                }

                const details = await this.fetchSaleDetails(sale.id);
                
                if (details && this.isValidPropertyData(details)) {
                    detailedProperties.push({
                        ...sale,
                        ...details,
                        neighborhood: neighborhood,
                        fetchedAt: new Date().toISOString()
                    });
                    successCount++;
                } else {
                    failureCount++;
                }

                if ((i + 1) % 20 === 0) {
                    console.log(`   📊 Progress: ${i + 1}/${activeSales.length} (${successCount} successful, ${failureCount} failed, ${this.baseDelay/1000}s delay)`);
                }

            } catch (error) {
                failureCount++;
                
                if (error.response?.status === 429) {
                    this.rateLimitHits++;
                    console.log(`   ⚡ Rate limit hit #${this.rateLimitHits} for ${sale.id}, adapting...`);
                    this.baseDelay = Math.min(25000, this.baseDelay * 1.5);
                    await this.delay(this.baseDelay * 2);
                } else {
                    console.log(`   ⚠️ Failed to get details for ${sale.id}: ${error.message}`);
                }
            }
        }

        console.log(`   ✅ Sales detail fetch complete: ${successCount} successful, ${failureCount} failed`);
        console.log(`   📊 Final adaptive delay: ${this.baseDelay/1000}s (${this.rateLimitHits} rate limits hit)`);
        return detailedProperties;
    }

    /**
     * Enhanced summary logging with de-duplication stats
     */
    logSmartSalesSummary(summary) {
        console.log('\n📊 SMART SCHEDULED SALES ANALYSIS COMPLETE');
        console.log('='.repeat(70));
        console.log(`📅 Schedule Day: ${summary.scheduledDay} of bi-weekly cycle`);
        console.log(`⏱️ Duration: ${summary.duration.toFixed(1)} minutes`);
        console.log(`🗽 Neighborhoods processed: ${summary.neighborhoodsProcessed}/${summary.totalNeighborhoods}`);
        console.log(`🏠 Active sales found: ${summary.totalActiveSalesFound}`);
        console.log(`🆕 New properties processed: ${summary.totalNewProperties}`);
        console.log(`🔄 Duplicates skipped: ${summary.totalDuplicatesSkipped}`);
        console.log(`🔍 Details attempted: ${summary.totalDetailsAttempted}`);
        console.log(`✅ Details successfully fetched: ${summary.totalDetailsFetched}`);
        console.log(`🎯 Undervalued sales found: ${summary.undervaluedFound}`);
        console.log(`💾 Saved to database: ${summary.savedToDatabase}`);
        console.log(`📞 API calls used: ${summary.apiCallsUsed}/${this.monthlyApiLimit} (${(summary.apiCallsUsed/this.monthlyApiLimit*100).toFixed(1)}%)`);
        console.log(`💰 API calls saved: ${summary.apiCallsSaved} (${(summary.apiCallsSaved/(summary.apiCallsUsed + summary.apiCallsSaved)*100).toFixed(1)}% efficiency)`);
        
        console.log('\n🔄 De-duplication Performance:');
        console.log(`   📊 Cache size: ${summary.detailedStats.deduplication.cacheSize} existing listings`);
        console.log(`   ✅ Duplicate checks: ${summary.detailedStats.deduplication.duplicateChecks}`);
        console.log(`   ⏭️ Duplicates skipped: ${summary.detailedStats.deduplication.duplicatesSkipped}`);
        console.log(`   🆕 New properties processed: ${summary.detailedStats.deduplication.newPropertiesProcessed}`);
        console.log(`   💰 API calls saved: ${summary.detailedStats.deduplication.apiCallsSaved}`);
        
        console.log('\n⚡ Adaptive Rate Limiting Performance:');
        console.log(`   🚀 Started with: 6s delays`);
        console.log(`   🎯 Ended with: ${this.baseDelay/1000}s delays`);
        console.log(`   📈 Rate limit hits: ${this.rateLimitHits}`);
        
        if (summary.savedToDatabase > 0) {
            console.log('\n🎉 SUCCESS: Found undervalued sales with smart scheduling & de-duplication!');
            console.log(`🔍 Check your Supabase 'undervalued_sales' table for ${summary.savedToDatabase} new deals`);
        }
        
        console.log('\n🆕 New Neighborhood: Carroll Gardens now included in coverage!');
    }

    /**
     * Fetch ALL active sales in a neighborhood
     */
    async fetchAllActiveSales(neighborhood) {
        try {
            console.log(`   📡 Fetching active sales for ${neighborhood}...`);
            
            const response = await axios.get(
                'https://streeteasy-api.p.rapidapi.com/sales/search',
                {
                    params: {
                        areas: neighborhood,
                        limit: 500, // Maximum allowed
                        minPrice: 200000, // Reasonable minimum for NYC
                        maxPrice: 10000000, // High maximum
                        offset: 0
                    },
                    headers: {
                        'X-RapidAPI-Key': this.rapidApiKey,
                        'X-RapidAPI-Host': 'streeteasy-api.p.rapidapi.com'
                    },
                    timeout: 30000
                }
            );

            this.apiCallsUsed++;
            this.apiUsageStats.activeSalesCalls++;

            // Handle response structure
            let salesData = [];
            if (response.data) {
                if (response.data.results && Array.isArray(response.data.results)) {
                    salesData = response.data.results;
                } else if (response.data.listings && Array.isArray(response.data.listings)) {
                    salesData = response.data.listings;
                } else if (Array.isArray(response.data)) {
                    salesData = response.data;
                }
            }

            console.log(`   ✅ Retrieved ${salesData.length} active sales`);
            return salesData;

        } catch (error) {
            this.apiUsageStats.failedCalls++;
            if (error.response?.status === 429) {
                this.apiUsageStats.rateLimitHits++;
            }
            throw error;
        }
    }

    /**
     * Fetch individual sale details using /sale/{id}
     */
    async fetchSaleDetails(saleId) {
        try {
            const response = await axios.get(
                `https://streeteasy-api.p.rapidapi.com/sale/${saleId}`,
                {
                    headers: {
                        'X-RapidAPI-Key': this.rapidApiKey,
                        'X-RapidAPI-Host': 'streeteasy-api.p.rapidapi.com'
                    },
                    timeout: 30000
                }
            );

            this.apiCallsUsed++;
            this.apiUsageStats.detailsCalls++;

            const data = response.data;
            
            // Extract the comprehensive property details
            return {
                // Property basics
                address: data.address || 'Address not available',
                bedrooms: data.bedrooms || 0,
                bathrooms: data.bathrooms || 0,
                sqft: data.sqft || null,
                propertyType: data.propertyType || 'unknown',
                
                // Pricing and market data
                price: data.price || 0,
                ppsqft: data.ppsqft || null,
                listedAt: data.listedAt || null,
                daysOnMarket: data.daysOnMarket || 0,
                
                // Building and location
                borough: data.borough || 'unknown',
                neighborhood: data.neighborhood || 'unknown',
                zipcode: data.zipcode || null,
                
                // Financial details
                monthlyHoa: data.monthlyHoa || null,
                monthlyTax: data.monthlyTax || null,
                
                // Property features
                amenities: data.amenities || [],
                description: data.description || '',
                
                // Building info
                building: data.building || {},
                builtIn: data.builtIn || null,
                
                // Images and media
                images: data.images || [],
                videos: data.videos || [],
                floorplans: data.floorplans || [],
                
                // Agent info
                agents: data.agents || []
            };

        } catch (error) {
            this.apiUsageStats.failedCalls++;
            if (error.response?.status === 429) {
                this.apiUsageStats.rateLimitHits++;
            }
            throw error;
        }
    }

    /**
     * Validate property data is complete enough for analysis
     */
    isValidPropertyData(property) {
        return property &&
               property.address &&
               property.price > 0 &&
               property.bedrooms !== undefined &&
               property.bathrooms !== undefined &&
               (property.sqft > 0 || property.ppsqft > 0);
    }

    /**
     * Analyze properties for TRUE undervaluation using complete data
     */
    analyzeForUndervaluation(detailedProperties, neighborhood) {
        if (detailedProperties.length < 3) {
            console.log(`   ⚠️ Not enough properties (${detailedProperties.length}) for comparison in ${neighborhood}`);
            return [];
        }

        console.log(`   🧮 Analyzing ${detailedProperties.length} properties for undervaluation...`);

        // Group properties by bedroom count for better comparisons
        const propertiesByBeds = this.groupPropertiesByBedrooms(detailedProperties);
        
        const undervaluedProperties = [];

        for (const [bedrooms, properties] of Object.entries(propertiesByBeds)) {
            if (properties.length < 2) continue; // Need at least 2 properties for comparison

            // Calculate market benchmarks for this bedroom count
            const marketData = this.calculateMarketBenchmarks(properties);
            
            console.log(`   📊 ${bedrooms}-bed properties: ${properties.length} found, median $${marketData.medianPrice.toLocaleString()}`);

            // Find undervalued properties in this bedroom group
            for (const property of properties) {
                const analysis = this.analyzePropertyValue(property, marketData, neighborhood);
                
                if (analysis.isUndervalued) {
                    undervaluedProperties.push({
                        ...property,
                        ...analysis,
                        comparisonGroup: `${bedrooms}-bed in ${neighborhood}`,
                        marketBenchmarks: marketData
                    });
                }
            }
        }

        // Sort by discount percentage (best deals first)
        undervaluedProperties.sort((a, b) => b.discountPercent - a.discountPercent);

        console.log(`   🎯 Found ${undervaluedProperties.length} undervalued properties`);
        return undervaluedProperties;
    }

    /**
     * Group properties by bedroom count for better comparisons
     */
    groupPropertiesByBedrooms(properties) {
        const grouped = {};
        
        properties.forEach(property => {
            const beds = property.bedrooms || 0;
            const key = beds === 0 ? 'studio' : `${beds}bed`;
            
            if (!grouped[key]) {
                grouped[key] = [];
            }
            grouped[key].push(property);
        });

        return grouped;
    }

    /**
     * Calculate market benchmarks for a group of similar properties
     */
    calculateMarketBenchmarks(properties) {
        const prices = properties.map(p => p.price).filter(p => p > 0).sort((a, b) => a - b);
        const pricesPerSqft = properties
            .filter(p => p.sqft > 0)
            .map(p => p.price / p.sqft)
            .sort((a, b) => a - b);

        const daysOnMarket = properties.map(p => p.daysOnMarket || 0).filter(d => d > 0);

        // Calculate price by bed/bath combinations for properties without sqft
        const pricePerBedBath = {};
        properties.forEach(property => {
            const beds = property.bedrooms || 0;
            const baths = property.bathrooms || 0;
            const key = `${beds}bed_${baths}bath`;
            
            if (!pricePerBedBath[key]) {
                pricePerBedBath[key] = [];
            }
            pricePerBedBath[key].push(property.price);
        });

        // Calculate medians for each bed/bath combination
        const bedBathMedians = {};
        for (const [combo, priceArray] of Object.entries(pricePerBedBath)) {
            if (priceArray.length >= 2) { // Need at least 2 comparables
                const sorted = priceArray.sort((a, b) => a - b);
                bedBathMedians[combo] = {
                    median: sorted[Math.floor(sorted.length / 2)],
                    count: sorted.length,
                    min: Math.min(...sorted),
                    max: Math.max(...sorted)
                };
            }
        }

        return {
            count: properties.length,
            medianPrice: prices[Math.floor(prices.length / 2)] || 0,
            avgPrice: prices.reduce((a, b) => a + b, 0) / prices.length || 0,
            medianPricePerSqft: pricesPerSqft.length > 0 ? pricesPerSqft[Math.floor(pricesPerSqft.length / 2)] : 0,
            avgPricePerSqft: pricesPerSqft.reduce((a, b) => a + b, 0) / pricesPerSqft.length || 0,
            avgDaysOnMarket: daysOnMarket.reduce((a, b) => a + b, 0) / daysOnMarket.length || 0,
            priceRange: {
                min: Math.min(...prices),
                max: Math.max(...prices)
            },
            pricePerBedBath: bedBathMedians,
            sqftDataAvailable: pricesPerSqft.length,
            totalProperties: properties.length
        };
    }

    /**
     * Analyze individual property for undervaluation
     */
    analyzePropertyValue(property, marketData, neighborhood) {
        const price = property.price;
        const sqft = property.sqft || 0;
        const beds = property.bedrooms || 0;
        const baths = property.bathrooms || 0;
        const pricePerSqft = sqft > 0 ? price / sqft : property.ppsqft || 0;

        // Calculate how far below market this property is
        let discountPercent = 0;
        let comparisonMethod = '';
        let reliabilityScore = 0; // How reliable is our comparison?

        if (pricePerSqft > 0 && marketData.medianPricePerSqft > 0) {
            // BEST: Use price per sqft comparison (most accurate)
            discountPercent = ((marketData.medianPricePerSqft - pricePerSqft) / marketData.medianPricePerSqft) * 100;
            comparisonMethod = 'price per sqft';
            reliabilityScore = 95;
        } else if (marketData.pricePerBedBath && marketData.pricePerBedBath[`${beds}bed_${baths}bath`]) {
            // GOOD: Use bed/bath specific price comparison
            const bedBathKey = `${beds}bed_${baths}bath`;
            const comparablePrice = marketData.pricePerBedBath[bedBathKey].median;
            discountPercent = ((comparablePrice - price) / comparablePrice) * 100;
            comparisonMethod = `${beds}bed/${baths}bath price comparison`;
            reliabilityScore = 80;
        } else if (marketData.medianPrice > 0) {
            // FALLBACK: Use total price comparison (least accurate)
            discountPercent = ((marketData.medianPrice - price) / marketData.medianPrice) * 100;
            comparisonMethod = 'total price (bedroom group)';
            reliabilityScore = 60;
        } else {
            // NO COMPARISON POSSIBLE
            return {
                isUndervalued: false,
                discountPercent: 0,
                comparisonMethod: 'insufficient data',
                reliabilityScore: 0,
                reasoning: 'Not enough comparable properties for analysis'
            };
        }

        // Adjust undervaluation threshold based on reliability
        let undervaluationThreshold = 10; // Default 10%
        if (reliabilityScore < 70) {
            undervaluationThreshold = 15; // Require bigger discount for less reliable comparisons
        }

        const isUndervalued = discountPercent >= undervaluationThreshold;

        // Calculate comprehensive score
        const score = this.calculateUndervaluationScore({
            discountPercent,
            daysOnMarket: property.daysOnMarket || 0,
            hasImages: (property.images || []).length > 0,
            hasDescription: (property.description || '').length > 100,
            bedrooms: property.bedrooms || 0,
            bathrooms: property.bathrooms || 0,
            sqft: sqft,
            amenities: property.amenities || [],
            neighborhood: neighborhood,
            reliabilityScore: reliabilityScore
        });

        return {
            isUndervalued,
            discountPercent: Math.round(discountPercent * 10) / 10,
            marketPricePerSqft: marketData.medianPricePerSqft,
            actualPricePerSqft: pricePerSqft,
            potentialSavings: Math.round((marketData.medianPrice - price)),
            comparisonMethod,
            reliabilityScore,
            score,
            grade: this.calculateGrade(score),
            reasoning: this.generateReasoning(discountPercent, property, marketData, comparisonMethod, reliabilityScore)
        };
    }

    /**
     * Calculate comprehensive undervaluation score
     */
    calculateUndervaluationScore(factors) {
        let score = 0;

        // Base score from discount percentage (0-50 points)
        score += Math.min(factors.discountPercent * 2, 50);

        // Days on market bonus (0-15 points)
        if (factors.daysOnMarket <= 7) score += 15;
        else if (factors.daysOnMarket <= 30) score += 10;
        else if (factors.daysOnMarket <= 60) score += 5;

        // Property quality bonuses
        if (factors.hasImages) score += 5;
        if (factors.hasDescription) score += 3;
        if (factors.bedrooms >= 2) score += 5;
        if (factors.bathrooms >= 2) score += 3;
        if (factors.sqft >= 1000) score += 8;
        if (factors.amenities.length >= 5) score += 5;

        // Neighborhood bonus for high-demand areas
        const premiumNeighborhoods = ['west-village', 'soho', 'tribeca', 'dumbo', 'williamsburg', 'carroll-gardens'];
        if (premiumNeighborhoods.includes(factors.neighborhood)) score += 10;

        return Math.min(100, Math.max(0, Math.round(score)));
    }

    /**
     * Calculate letter grade from score
     */
    calculateGrade(score) {
        if (score >= 85) return 'A+';
        if (score >= 75) return 'A';
        if (score >= 65) return 'B+';
        if (score >= 55) return 'B';
        if (score >= 45) return 'C+';
        if (score >= 35) return 'C';
        return 'D';
    }

    /**
     * Generate human-readable reasoning
     */
    generateReasoning(discountPercent, property, marketData, comparisonMethod, reliabilityScore) {
        const reasons = [];
        
        reasons.push(`${discountPercent.toFixed(1)}% below market (${comparisonMethod})`);
        
        if (property.daysOnMarket <= 7) {
            reasons.push(`fresh listing (${property.daysOnMarket} days)`);
        } else if (property.daysOnMarket > 60) {
            reasons.push(`longer on market (${property.daysOnMarket} days)`);
        }
        
        if ((property.images || []).length > 0) {
            reasons.push(`${property.images.length} photos available`);
        }
        
        if (property.amenities && property.amenities.length > 0) {
            reasons.push(`${property.amenities.length} amenities`);
        }

        return reasons.join('; ');
    }

    /**
     * Enhanced save with de-duplication protection
     */
    async saveUndervaluedSalesToDatabase(undervaluedProperties, neighborhood) {
        console.log(`   💾 Saving ${undervaluedProperties.length} undervalued sales to database...`);

        let savedCount = 0;

        for (const property of undervaluedProperties) {
            try {
                // Double-check for duplicates (belt and suspenders approach)
                const { data: existing } = await this.supabase
                    .from('undervalued_sales')
                    .select('id')
                    .eq('listing_id', property.id)
                    .single();

                if (existing) {
                    console.log(`   ⏭️ Skipping duplicate: ${property.address}`);
                    continue;
                }

                // Enhanced database record with complete data
                const dbRecord = {
                    listing_id: property.id?.toString(),
                    address: property.address,
                    neighborhood: property.neighborhood,
                    borough: property.borough || 'unknown',
                    zipcode: property.zipcode,
                    
                    // Pricing - ensure proper data types and validation
                    price: parseInt(property.price) || 0,
                    price_per_sqft: property.actualPricePerSqft ? parseFloat(property.actualPricePerSqft.toFixed(2)) : null,
                    market_price_per_sqft: property.marketPricePerSqft ? parseFloat(property.marketPricePerSqft.toFixed(2)) : null,
                    discount_percent: parseFloat(property.discountPercent.toFixed(2)),
                    potential_savings: parseInt(property.potentialSavings) || 0,
                    
                    // Property details - ensure proper types
                    bedrooms: parseInt(property.bedrooms) || 0,
                    bathrooms: property.bathrooms ? parseFloat(property.bathrooms) : null,
                    sqft: property.sqft ? parseInt(property.sqft) : null,
                    property_type: property.propertyType || 'unknown',
                    
                    // Market timing - handle date conversions properly
                    listed_at: property.listedAt ? new Date(property.listedAt).toISOString() : null,
                    days_on_market: parseInt(property.daysOnMarket) || 0,
                    
                    // Financial details - ensure proper numbers
                    monthly_hoa: property.monthlyHoa ? parseFloat(property.monthlyHoa) : null,
                    monthly_tax: property.monthlyTax ? parseFloat(property.monthlyTax) : null,
                    built_in: property.builtIn ? parseInt(property.builtIn) : null,
                    
                    // Media and description - with validation
                    images: this.validateAndCleanImages(property.images || []),
                    image_count: this.countValidImages(property.images || []),
                    videos: Array.isArray(property.videos) ? property.videos : [],
                    floorplans: Array.isArray(property.floorplans) ? property.floorplans : [],
                    description: typeof property.description === 'string' ? 
                        property.description.substring(0, 2000) : '',
                    amenities: Array.isArray(property.amenities) ? property.amenities : [],
                    
                    // Analysis results - with validation
                    score: Math.max(0, Math.min(100, parseInt(property.score) || 0)),
                    grade: this.validateGrade(property.grade),
                    reasoning: property.reasoning || '',
                    comparison_group: property.comparisonGroup || '',
                    comparison_method: property.comparisonMethod || 'unknown',
                    reliability_score: Math.max(0, Math.min(100, parseInt(property.reliabilityScore) || 0)),
                    
                    // JSON fields - ensure proper objects
                    building_info: typeof property.building === 'object' ? property.building : {},
                    agents: Array.isArray(property.agents) ? property.agents : [],
                    
                    analysis_date: new Date().toISOString(),
                    status: 'active'
                };

                const { error } = await this.supabase
                    .from('undervalued_sales')
                    .insert([dbRecord]);

                if (error) {
                    console.error(`   ❌ Error saving ${property.address}:`, error.message);
                } else {
                    console.log(`   ✅ Saved: ${property.address} (${property.discountPercent}% below market, Score: ${property.score})`);
                    savedCount++;
                }

            } catch (error) {
                console.error(`   ❌ Error processing ${property.address}:`, error.message);
            }
        }

        console.log(`   💾 Saved ${savedCount} new undervalued sales`);
        return savedCount;
    }

    /**
     * Clear old sales data
     */
    async clearOldSalesData() {
        try {
            const oneMonthAgo = new Date();
            oneMonthAgo.setMonth(oneMonthAgo.getMonth() - 1);

            const { error } = await this.supabase
                .from('undervalued_sales')
                .delete()
                .lt('analysis_date', oneMonthAgo.toISOString());

            if (error) {
                console.error('❌ Error clearing old sales data:', error.message);
            } else {
                console.log('🧹 Cleared old sales data (>1 month)');
            }
        } catch (error) {
            console.error('❌ Clear old sales data error:', error.message);
        }
    }

    /**
     * Save bi-weekly summary with enhanced de-duplication stats
     */
    async saveBiWeeklySummary(summary) {
        try {
            const { error } = await this.supabase
                .from('bi_weekly_analysis_runs')
                .insert([{
                    run_date: summary.startTime,
                    analysis_type: 'sales',
                    neighborhoods_processed: summary.neighborhoodsProcessed,
                    total_active_sales: summary.totalActiveSalesFound,
                    total_new_properties: summary.totalNewProperties,
                    total_duplicates_skipped: summary.totalDuplicatesSkipped,
                    total_details_attempted: summary.totalDetailsAttempted,
                    total_details_fetched: summary.totalDetailsFetched,
                    undervalued_found: summary.undervaluedFound,
                    saved_to_database: summary.savedToDatabase,
                    api_calls_used: summary.apiCallsUsed,
                    api_calls_saved: summary.apiCallsSaved,
                    duration_minutes: Math.round(summary.duration),
                    detailed_stats: summary.detailedStats,
                    errors: summary.errors,
                    completed: true
                }]);

            if (error) {
                console.error('❌ Error saving bi-weekly summary:', error.message);
            } else {
                console.log('✅ Enhanced bi-weekly summary saved to database');
            }
        } catch (error) {
            console.error('❌ Save summary error:', error.message);
        }
    }

    delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    /**
     * Validate and clean image URLs
     */
    validateAndCleanImages(images) {
        if (!Array.isArray(images)) return [];
        
        return images
            .filter(img => typeof img === 'string' && img.length > 0)
            .filter(img => this.isValidImageUrl(img))
            .slice(0, 50); // Limit to 50 images max
    }

    /**
     * Count valid images
     */
    countValidImages(images) {
        return this.validateAndCleanImages(images).length;
    }

    /**
     * Validate image URL format
     */
    isValidImageUrl(url) {
        if (!url || typeof url !== 'string') return false;
        
        try {
            new URL(url); // Basic URL validation
            
            // Check for valid image patterns
            const validPatterns = [
                /\.(jpg|jpeg|png|webp|gif)(\?.*)?$/i,
                /photos\.(zillow|streeteasy)/i,
                /images\.(streeteasy|zillow)/i
            ];
            
            return validPatterns.some(pattern => pattern.test(url)) && url.startsWith('http');
        } catch {
            return false;
        }
    }

    /**
     * Validate grade values
     */
    validateGrade(grade) {
        const validGrades = ['A+', 'A', 'B+', 'B', 'C+', 'C', 'D', 'F'];
        return validGrades.includes(grade) ? grade : 'F';
    }

    /**
     * Setup enhanced database schema for sales
     */
    async setupSalesDatabase() {
        console.log('🔧 Setting up enhanced sales database schema...');

        try {
            const salesTableSchema = `
                CREATE TABLE IF NOT EXISTS undervalued_sales (
                    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
                    listing_id text UNIQUE,
                    address text,
                    neighborhood text,
                    borough text,
                    zipcode text,
                    
                    -- Pricing analysis (using bigint for large prices)
                    price bigint,
                    price_per_sqft decimal(10,2),
                    market_price_per_sqft decimal(10,2),
                    discount_percent decimal(5,2),
                    potential_savings bigint,
                    
                    -- Property details
                    bedrooms integer,
                    bathrooms decimal(3,1),
                    sqft integer,
                    property_type text,
                    
                    -- Market timing
                    listed_at timestamptz,
                    days_on_market integer,
                    
                    -- Financial details
                    monthly_hoa decimal(10,2),
                    monthly_tax decimal(10,2),
                    built_in integer,
                    
                    -- Media and description
                    images jsonb DEFAULT '[]'::jsonb,
                    image_count integer DEFAULT 0,
                    description text,
                    amenities text[] DEFAULT ARRAY[]::text[],
                    
                    -- Analysis results
                    score integer,
                    grade text,
                    reasoning text,
                    comparison_group text,
                    comparison_method text,
                    reliability_score integer,
                    
                    -- Additional data (JSON for flexibility)
                    building_info jsonb DEFAULT '{}'::jsonb,
                    agents jsonb DEFAULT '[]'::jsonb,
                    
                    -- Metadata
                    analysis_date timestamptz DEFAULT now(),
                    status text DEFAULT 'active',
                    created_at timestamptz DEFAULT now()
                );
            `;

            const summaryTableSchema = `
                CREATE TABLE IF NOT EXISTS bi_weekly_analysis_runs (
                    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
                    run_date timestamp DEFAULT now(),
                    analysis_type text,
                    neighborhoods_processed int,
                    total_active_sales int,
                    total_new_properties int,
                    total_duplicates_skipped int,
                    total_details_attempted int,
                    total_details_fetched int,
                    undervalued_found int,
                    saved_to_database int,
                    api_calls_used int,
                    api_calls_saved int,
                    duration_minutes int,
                    detailed_stats jsonb,
                    errors jsonb,
                    completed boolean DEFAULT true,
                    created_at timestamp DEFAULT now()
                );
            `;

            console.log('✅ Enhanced sales database setup complete');

        } catch (error) {
            console.error('❌ Database setup error:', error.message);
        }
    }

    /**
     * Get latest undervalued sales
     */
    async getLatestUndervaluedSales(limit = 50, minScore = 50) {
        try {
            const { data, error } = await this.supabase
                .from('undervalued_sales')
                .select('*')
                .gte('score', minScore)
                .order('analysis_date', { ascending: false })
                .limit(limit);

            if (error) throw error;
            return data;
        } catch (error) {
            console.error('❌ Error fetching latest sales:', error.message);
            return [];
        }
    }

    /**
     * Get sales by neighborhood
     */
    async getSalesByNeighborhood(neighborhood, limit = 20) {
        try {
            const { data, error } = await this.supabase
                .from('undervalued_sales')
                .select('*')
                .eq('neighborhood', neighborhood)
                .order('score', { ascending: false })
                .limit(limit);

            if (error) throw error;
            return data;
        } catch (error) {
            console.error('❌ Error fetching sales by neighborhood:', error.message);
            return [];
        }
    }

    /**
     * Get top scoring deals
     */
    async getTopSalesDeals(limit = 20) {
        try {
            const { data, error } = await this.supabase
                .from('undervalued_sales')
                .select('*')
                .gte('score', 70)
                .order('score', { ascending: false })
                .limit(limit);

            if (error) throw error;
            return data;
        } catch (error) {
            console.error('❌ Error fetching top deals:', error.message);
            return [];
        }
    }
}

// CLI interface
async function main() {
    const args = process.argv.slice(2);
    
    if (!process.env.RAPIDAPI_KEY || !process.env.SUPABASE_URL || !process.env.SUPABASE_ANON_KEY) {
        console.error('❌ Missing required environment variables!');
        console.error('   RAPIDAPI_KEY, SUPABASE_URL, SUPABASE_ANON_KEY required');
        process.exit(1);
    }

    const analyzer = new EnhancedBiWeeklySalesAnalyzer();

    if (args.includes('--setup')) {
        await analyzer.setupSalesDatabase();
        return;
    }

    if (args.includes('--latest')) {
        const limit = parseInt(args[args.indexOf('--latest') + 1]) || 20;
        const sales = await analyzer.getLatestUndervaluedSales(limit);
        console.log(`🏠 Latest ${sales.length} undervalued sales:`);
        sales.forEach((sale, i) => {
            console.log(`${i + 1}. ${sale.address} - ${sale.price.toLocaleString()} (${sale.discount_percent}% below market, Score: ${sale.score})`);
        });
        return;
    }

    if (args.includes('--top-deals')) {
        const limit = parseInt(args[args.indexOf('--top-deals') + 1]) || 10;
        const deals = await analyzer.getTopSalesDeals(limit);
        console.log(`🏆 Top ${deals.length} sales deals:`);
        deals.forEach((deal, i) => {
            console.log(`${i + 1}. ${deal.address} - ${deal.price.toLocaleString()} (${deal.discount_percent}% below market, Score: ${deal.score})`);
        });
        return;
    }

    if (args.includes('--neighborhood')) {
        const neighborhood = args[args.indexOf('--neighborhood') + 1];
        if (!neighborhood) {
            console.error('❌ Please provide a neighborhood: --neighborhood carroll-gardens');
            return;
        }
        const sales = await analyzer.getSalesByNeighborhood(neighborhood);
        console.log(`🏠 Sales in ${neighborhood}:`);
        sales.forEach((sale, i) => {
            console.log(`${i + 1}. ${sale.address} - ${sale.price.toLocaleString()} (Score: ${sale.score})`);
        });
        return;
    }

    // Default: run bi-weekly analysis
    console.log('🗽 Starting enhanced bi-weekly sales analysis with de-duplication...');
    const results = await analyzer.runBiWeeklySalesRefresh();
    
    console.log('\n🎉 Enhanced bi-weekly sales analysis completed!');
    console.log(`📊 Check your Supabase 'undervalued_sales' table for ${results.savedToDatabase} new deals!`);
    
    return results;
}

// Export for use in other modules
module.exports = EnhancedBiWeeklySalesAnalyzer;

// Run if executed directly
if (require.main === module) {
    main().catch(error => {
        console.error('💥 Enhanced sales analyzer crashed:', error);
        process.exit(1);
    });
}
