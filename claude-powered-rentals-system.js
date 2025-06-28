// claude-powered-rentals-system.js
// COMPLETE CLAUDE-POWERED RENTAL ANALYSIS SYSTEM
// Full replacement for rent-stabilized-undervalued-system.js with comprehensive functionality
require('dotenv').config();
const axios = require('axios');
const { createClient } = require('@supabase/supabase-js');
const ClaudeMarketAnalyzer = require('./claude-market-analyzer');

/**
 * COMPLETE CLAUDE-POWERED RENTAL SYSTEM
 * Features: Smart caching, DHCR cross-referencing, dual-table saving, CLI interface, error handling
 */
class ClaudePoweredRentalSystem {
    constructor() {
        this.supabase = createClient(
            process.env.SUPABASE_URL,
            process.env.SUPABASE_ANON_KEY
        );
        
        this.claudeAnalyzer = new ClaudeMarketAnalyzer();
        this.rapidApiKey = process.env.RAPIDAPI_KEY;
        
        // SEPARATE THRESHOLD CONFIGURATION
        this.testNeighborhood = process.env.TEST_NEIGHBORHOOD;
        this.stabilizationThreshold = parseInt(process.env.RENT_STABILIZED_CONFIDENCE_THRESHOLD) || 40;
        this.undervaluationThreshold = parseFloat(process.env.UNDERVALUATION_THRESHOLD) || 15;
        this.stabilizedUndervaluationThreshold = parseFloat(process.env.STABILIZED_UNDERVALUATION_THRESHOLD) || -100;
        this.maxListingsPerNeighborhood = parseInt(process.env.MAX_LISTINGS_PER_NEIGHBORHOOD) || 500;
        
        // Rate limiting and tracking
        this.apiCallsUsed = 0;
        this.baseDelay = 1500;
        this.rateLimitHits = 0;
        
        // DHCR buildings cache
        this.rentStabilizedBuildings = [];
        
        // NYC neighborhoods (comprehensive list)
        this.priorityNeighborhoods = [
            // Manhattan
            'east-village', 'west-village', 'lower-east-side', 'chinatown', 'little-italy',
            'nolita', 'soho', 'tribeca', 'financial-district', 'two-bridges',
            'chelsea', 'gramercy', 'kips-bay', 'murray-hill', 'midtown-east',
            'midtown-west', 'hells-kitchen', 'upper-east-side', 'upper-west-side',
            'morningside-heights', 'hamilton-heights', 'washington-heights', 'inwood',
            'greenwich-village', 'flatiron', 'noho', 'bowery',
            
            // Brooklyn  
            'williamsburg', 'greenpoint', 'bushwick', 'bedford-stuyvesant',
            'crown-heights', 'prospect-heights', 'park-slope', 'gowanus',
            'carroll-gardens', 'cobble-hill', 'brooklyn-heights', 'dumbo',
            'fort-greene', 'clinton-hill', 'boerum-hill', 'red-hook',
            'prospect-lefferts-gardens', 'sunset-park', 'bay-ridge', 'bensonhurst',
            
            // Queens
            'long-island-city', 'astoria', 'sunnyside', 'woodside',
            'jackson-heights', 'elmhurst', 'corona', 'flushing',
            'forest-hills', 'ridgewood', 'maspeth', 'rego-park',
            
            // Bronx
            'south-bronx', 'mott-haven', 'concourse', 'highbridge',
            'fordham', 'university-heights', 'morrisania', 'melrose'
        ];
        
        // Borough mapping
        this.boroughMap = {
            manhattan: ['east-village', 'west-village', 'lower-east-side', 'chinatown', 'little-italy', 'nolita', 'soho', 'tribeca', 'financial-district', 'two-bridges', 'chelsea', 'gramercy', 'kips-bay', 'murray-hill', 'midtown-east', 'midtown-west', 'hells-kitchen', 'upper-east-side', 'upper-west-side', 'morningside-heights', 'hamilton-heights', 'washington-heights', 'inwood', 'greenwich-village', 'flatiron', 'noho', 'bowery'],
            brooklyn: ['williamsburg', 'greenpoint', 'bushwick', 'bedford-stuyvesant', 'crown-heights', 'prospect-heights', 'park-slope', 'gowanus', 'carroll-gardens', 'cobble-hill', 'brooklyn-heights', 'dumbo', 'fort-greene', 'clinton-hill', 'boerum-hill', 'red-hook', 'prospect-lefferts-gardens', 'sunset-park', 'bay-ridge', 'bensonhurst'],
            queens: ['long-island-city', 'astoria', 'sunnyside', 'woodside', 'jackson-heights', 'elmhurst', 'corona', 'flushing', 'forest-hills', 'ridgewood', 'maspeth', 'rego-park'],
            bronx: ['south-bronx', 'mott-haven', 'concourse', 'highbridge', 'fordham', 'university-heights', 'morrisania', 'melrose']
        };
        
        console.log('🤖 Claude-Powered Rental System initialized');
        console.log(`🔒 Stabilization threshold: ${this.stabilizationThreshold}%`);
        console.log(`📊 Regular undervaluation threshold: ${this.undervaluationThreshold}%`);
        console.log(`🏠 Stabilized undervaluation threshold: ${this.stabilizedUndervaluationThreshold}%`);
    }

    /**
     * MAIN ENTRY POINT: Run complete rental analysis
     */
    async runComprehensiveRentalAnalysis() {
        console.log('\n🚀 CLAUDE-POWERED RENTAL ANALYSIS STARTING...');
        console.log('=' .repeat(60));
        
        const startTime = Date.now();
        const results = {
            undervaluedRentals: 0,
            rentStabilizedFound: 0,
            undervaluedStabilized: 0,
            totalAnalyzed: 0,
            neighborhoodsProcessed: 0,
            apiCallsUsed: 0,
            errors: [],
            cacheEfficiency: 0
        };
        
        try {
            // Step 1: Load DHCR rent-stabilized buildings
            console.log('🏢 Loading rent-stabilized buildings from DHCR data...');
            this.rentStabilizedBuildings = await this.loadRentStabilizedBuildings();
            console.log(`   ✅ Loaded ${this.rentStabilizedBuildings.length} rent-stabilized buildings`);
            
            // Step 2: Determine neighborhoods to process
            const neighborhoods = this.testNeighborhood 
                ? [this.testNeighborhood] 
                : this.priorityNeighborhoods.slice(0, 20); // Limit for efficiency
            
            console.log(`🎯 Processing ${neighborhoods.length} neighborhoods...`);
            
            // Step 3: Process each neighborhood with caching
            let totalCacheHits = 0;
            let totalApiCalls = 0;
            
            for (const neighborhood of neighborhoods) {
                try {
                    console.log(`\n📍 Analyzing ${neighborhood}...`);
                    
                    const neighborhoodResults = await this.analyzeNeighborhoodWithCaching(neighborhood);
                    
                    // Update totals
                    results.undervaluedRentals += neighborhoodResults.undervaluedCount;
                    results.rentStabilizedFound += neighborhoodResults.stabilizedCount;
                    results.undervaluedStabilized += neighborhoodResults.undervaluedStabilizedCount;
                    results.totalAnalyzed += neighborhoodResults.totalAnalyzed;
                    results.neighborhoodsProcessed++;
                    
                    totalCacheHits += neighborhoodResults.cacheHits;
                    totalApiCalls += neighborhoodResults.apiCalls || 0;
                    
                    console.log(`   ✅ ${neighborhood}: ${neighborhoodResults.undervaluedCount} undervalued, ${neighborhoodResults.stabilizedCount} stabilized (${neighborhoodResults.cacheHits} cache hits)`);
                    
                } catch (error) {
                    console.error(`   ❌ Error in ${neighborhood}: ${error.message}`);
                    results.errors.push({ neighborhood, error: error.message });
                }
                
                // Rate limiting between neighborhoods
                await this.delay(this.baseDelay);
            }
            
            // Calculate efficiency
            results.cacheEfficiency = totalApiCalls > 0 ? Math.round((totalCacheHits / (totalCacheHits + totalApiCalls)) * 100) : 0;
            
            // Final results
            const duration = (Date.now() - startTime) / 1000;
            results.apiCallsUsed = this.apiCallsUsed;
            
            console.log('\n🎉 CLAUDE ANALYSIS COMPLETE!');
            console.log('=' .repeat(60));
            console.log(`⏱️  Duration: ${Math.round(duration)}s`);
            console.log(`📊 Total analyzed: ${results.totalAnalyzed} properties`);
            console.log(`🏠 Undervalued rentals: ${results.undervaluedRentals}`);
            console.log(`🔒 Rent-stabilized found: ${results.rentStabilizedFound}`);
            console.log(`💎 Undervalued + Stabilized: ${results.undervaluedStabilized}`);
            console.log(`🤖 Claude API calls: ${results.apiCallsUsed}`);
            console.log(`💰 Estimated cost: $${(results.apiCallsUsed * 0.0006).toFixed(3)}`);
            console.log(`⚡ Cache efficiency: ${results.cacheEfficiency}%`);
            
            return results;
            
        } catch (error) {
            console.error('💥 ANALYSIS FAILED:', error.message);
            throw error;
        }
    }

    /**
     * Load rent-stabilized buildings from Supabase (with batch loading)
     */
    async loadRentStabilizedBuildings() {
    try {
        let allBuildings = [];
        let offset = 0;
        const batchSize = 1000;  // Keep reasonable batch size
        let hasMoreData = true;
        
        while (hasMoreData) {
            console.log(`   📊 Loading batch starting at offset ${offset}...`);
            
            const { data, error } = await this.supabase
                .from('rent_stabilized_buildings')
                .select('*')
                .range(offset, offset + batchSize - 1)
                .order('id');
            
            if (error) {
                console.error(`   ❌ Error loading buildings at offset ${offset}:`, error.message);
                throw error;
            }
            
            // ROBUST STOPPING CONDITION:
            if (!data || data.length === 0) {
                hasMoreData = false;
                break;
            }
            
            allBuildings = allBuildings.concat(data);
            console.log(`   ✅ Loaded ${data.length} buildings (total: ${allBuildings.length})`);
            
            // DYNAMIC CONTINUATION:
            if (data.length < batchSize) {
                // Got fewer records than requested = reached the end
                hasMoreData = false;
            }
            
            offset += batchSize;
            
            // SAFETY CHECK (higher limit for 50k+ buildings):
            if (offset > 100000) {
                console.log('   ⚠️ Reached safety limit of 100,000 buildings');
                hasMoreData = false;
            }
        }
        
        return allBuildings;
            
        } catch (error) {
            console.error('Failed to load rent-stabilized buildings:', error.message);
            console.log('   🔄 Continuing without DHCR data - Claude will use other indicators');
            return [];
        }
    }

    /**
     * Analyze neighborhood with smart caching
     */
    async analyzeNeighborhoodWithCaching(neighborhood) {
        const results = {
            undervaluedCount: 0,
            stabilizedCount: 0,
            undervaluedStabilizedCount: 0,
            totalAnalyzed: 0,
            cacheHits: 0,
            apiCalls: 0
        };
        
        try {
            // Step 1: Get rental listings with caching
            const listingResults = await this.fetchNeighborhoodListingsWithCache(neighborhood);
            results.cacheHits += listingResults.cacheHits;
            results.apiCalls += 1; // API call for listings
            
            console.log(`   📋 Found ${listingResults.totalFound} listings (${listingResults.cacheHits} cache hits, ${listingResults.newListings.length} new)`);
            
            if (listingResults.totalFound === 0) {
                console.log(`   ⚠️ No listings found for ${neighborhood}`);
                return results;
            }
            
            // Step 2: Get detailed info for new listings only
            const detailedListings = await this.fetchDetailedListingsWithCache(listingResults.newListings, neighborhood);
            results.apiCalls += listingResults.newListings.length;
            
            console.log(`   📊 Got details for ${detailedListings.length} new properties`);
            
            // Step 3: Get all cached + new listings for analysis
            const allListings = await this.getCachedListingsForNeighborhood(neighborhood);
            console.log(`   🔄 Total properties for analysis: ${allListings.length} (including cached)`);
            
            if (allListings.length < 5) {
                console.log(`   ⚠️ Insufficient properties for analysis (${allListings.length})`);
                return results;
            }
            
            // Step 4: Claude analysis for each property
            const analyzedProperties = [];
            const analysisLimit = Math.min(50, allListings.length); // Limit for efficiency
            
            for (const property of allListings.slice(0, analysisLimit)) {
                try {
                    const analysis = await this.claudeAnalyzer.analyzeRentalsUndervaluation(
                        property,
                        allListings, // Use all as comparables
                        this.rentStabilizedBuildings, // DHCR cross-reference
                        neighborhood,
                        { undervaluationThreshold: this.undervaluationThreshold }
                    );
                    
                    if (analysis.success) {
                        const isUndervalued = analysis.percentBelowMarket >= this.undervaluationThreshold;
                        const isStabilized = analysis.rentStabilizedProbability >= this.stabilizationThreshold;
                        
                        const analyzedProperty = {
                            ...property,
                            // Undervaluation analysis
                            estimatedMarketRent: analysis.estimatedMarketRent,
                            percentBelowMarket: analysis.percentBelowMarket,
                            potentialSavings: Math.max(0, analysis.estimatedMarketRent - property.price),
                            undervaluationConfidence: analysis.confidence,
                            
                            // Rent stabilization analysis
                            rentStabilizedProbability: analysis.rentStabilizedProbability,
                            rentStabilizedFactors: analysis.rentStabilizedFactors || [],
                            rentStabilizedExplanation: analysis.rentStabilizedExplanation,
                            
                            // Classifications
                            isUndervalued: isUndervalued,
                            isRentStabilized: isStabilized,
                            isUndervaluedStabilized: isUndervalued && isStabilized,
                            
                            // Analysis metadata
                            analysisMethod: 'claude_ai',
                            reasoning: analysis.reasoning,
                            comparablesUsed: allListings.length
                        };
                        
                        analyzedProperties.push(analyzedProperty);
                        
                        // Update counts
                        if (isUndervalued) results.undervaluedCount++;
                        if (isStabilized) results.stabilizedCount++;
                        if (isUndervalued && isStabilized) results.undervaluedStabilizedCount++;
                        
                        console.log(`     ✅ ${property.address}: ${analysis.percentBelowMarket.toFixed(1)}% below market, ${analysis.rentStabilizedProbability}% stabilized`);
                    } else {
                        console.log(`     ⚠️ Analysis failed for ${property.address}: ${analysis.error}`);
                    }
                    
                } catch (error) {
                    console.warn(`     ⚠️ Analysis exception for ${property.address}: ${error.message}`);
                }
                
                results.totalAnalyzed++;
                
                // Rate limiting between properties
                await this.delay(100);
            }
            
            // Step 5: Save results using separate thresholds
            if (analyzedProperties.length > 0) {
                await this.saveAnalyzedProperties(analyzedProperties, neighborhood);
            }
            
            // Step 6: Update cache with analysis results
            await this.updateCacheWithResults(detailedListings, analyzedProperties);
            
            return results;
            
        } catch (error) {
            console.error(`   ❌ Neighborhood analysis failed: ${error.message}`);
            throw error;
        }
    }

    /**
     * Fetch neighborhood listings with smart caching
     */
    async fetchNeighborhoodListingsWithCache(neighborhood) {
        try {
            this.apiCallsUsed++;
            
            const response = await axios.get('https://streeteasy-api.p.rapidapi.com/rentals/search', {
                params: {
                    areas: neighborhood,
                    limit: this.maxListingsPerNeighborhood
                },
                headers: {
                    'X-RapidAPI-Key': this.rapidApiKey,
                    'X-RapidAPI-Host': 'streeteasy-api.p.rapidapi.com'
                },
                timeout: 30000
            });
            
            let listings = [];
console.log(`   🔍 Response structure:`, Object.keys(response.data || {}));
console.log(`   🔍 Response sample:`, JSON.stringify(response.data).substring(0, 300));

if (response.data) {
    if (response.data.listings) {
        listings = response.data.listings;
    } else if (response.data.results) {
        listings = response.data.results;
    } else if (response.data.rentals) {
        listings = response.data.rentals;
    } else if (Array.isArray(response.data)) {
        listings = response.data;
    }
}

console.log(`   🔍 Extracted ${listings.length} listings from response`);
            
           // Filter valid listings (only check what's available in initial response)
const validListings = listings.filter(listing => 
    listing && 
    listing.id &&
    listing.price > 0
);

console.log(`   🔍 After filtering: ${validListings.length} valid listings`);
            
            // Check cache for existing listings
            const listingIds = validListings.map(l => l.id?.toString()).filter(Boolean);
            const cachedIds = await this.getCachedListingIds(listingIds);
            
            const newListings = validListings.filter(listing => 
                !cachedIds.includes(listing.id?.toString())
            );
            
            // Update timestamps for all seen listings
            await this.updateListingTimestamps(validListings, neighborhood);
            
            return {
                totalFound: validListings.length,
                newListings: newListings,
                cacheHits: cachedIds.length
            };
            
        } catch (error) {
            if (error.response?.status === 429) {
                this.rateLimitHits++;
                console.error(`   ❌ Rate limit hit for ${neighborhood}: ${error.message}`);
                await this.delay(5000); // Wait 5 seconds on rate limit
            } else {
                console.error(`   ❌ Failed to fetch listings for ${neighborhood}: ${error.message}`);
            }
            return { totalFound: 0, newListings: [], cacheHits: 0 };
        }
    }

    /**
     * Get cached listing IDs
     */
    async getCachedListingIds(listingIds) {
        try {
            if (listingIds.length === 0) return [];
            
            const { data, error } = await this.supabase
                .from('rental_market_cache')
                .select('listing_id')
                .in('listing_id', listingIds);
            
            if (error) throw error;
            
            return (data || []).map(row => row.listing_id);
        } catch (error) {
            console.warn(`   ⚠️ Cache check failed: ${error.message}`);
            return [];
        }
    }

    /**
     * Update listing timestamps (for sold/rented detection)
     */
    async updateListingTimestamps(listings, neighborhood) {
        try {
            const updates = listings.map(listing => ({
                listing_id: listing.id?.toString(),
                address: listing.address,
                monthly_rent: listing.price,
                bedrooms: listing.bedrooms,
                bathrooms: listing.bathrooms,
                neighborhood: neighborhood,
                borough: this.getBoroughFromNeighborhood(neighborhood),
                market_status: 'pending',
                last_seen_in_search: new Date().toISOString(),
                last_checked: new Date().toISOString(),
                times_seen: 1
            }));
            
            const { error } = await this.supabase
                .from('rental_market_cache')
                .upsert(updates, { 
                    onConflict: 'listing_id',
                    updateColumns: ['last_seen_in_search', 'last_checked']
                });
            
            if (error) {
                console.warn(`   ⚠️ Timestamp update failed: ${error.message}`);
            }
        } catch (error) {
            console.warn(`   ⚠️ Exception updating timestamps: ${error.message}`);
        }
    }

    /**
     * Get cached listings for neighborhood analysis
     */
    async getCachedListingsForNeighborhood(neighborhood) {
        try {
            const { data, error } = await this.supabase
                .from('rental_market_cache')
                .select('*')
                .eq('neighborhood', neighborhood)
                .neq('market_status', 'fetch_failed')
                .order('last_seen_in_search', { ascending: false })
                .limit(200); // Reasonable limit for analysis
            
            if (error) throw error;
            
            return (data || []).map(row => ({
                id: row.listing_id,
                address: row.address,
                price: row.monthly_rent,
                bedrooms: row.bedrooms,
                bathrooms: row.bathrooms,
                sqft: row.sqft,
                neighborhood: row.neighborhood,
                amenities: row.amenities || [],
                description: row.description || ''
            }));
        } catch (error) {
            console.warn(`   ⚠️ Failed to get cached listings: ${error.message}`);
            return [];
        }
    }

    /**
     * Fetch detailed property information with caching
     */
    async fetchDetailedListingsWithCache(listings, neighborhood) {
        const detailed = [];
        
        for (const listing of listings.slice(0, 25)) { // Limit for efficiency
            try {
                this.apiCallsUsed++;
                
                const response = await axios.get(`https://streeteasy-api.p.rapidapi.com/rentals/${listing.id}`, {
                    headers: {
                        'X-RapidAPI-Key': this.rapidApiKey,
                        'X-RapidAPI-Host': 'streeteasy-api.p.rapidapi.com'
                    },
                    timeout: 15000
                });
                
                if (response.data) {
                    const detailedListing = {
                        ...listing,
                        ...response.data,
                        neighborhood: neighborhood
                    };
                    
                    detailed.push(detailedListing);
                    
                    // Cache the detailed listing
                    await this.cacheDetailedListing(detailedListing, neighborhood);
                }
                
            } catch (error) {
                if (error.response?.status === 429) {
                    this.rateLimitHits++;
                    console.warn(`     ⚠️ Rate limit hit for listing ${listing.id}`);
                    await this.delay(2000);
                } else {
                    console.warn(`     ⚠️ Failed to fetch details for ${listing.id}: ${error.message}`);
                }
            }
            
            await this.delay(200); // Rate limiting
        }
        
        return detailed;
    }

    /**
     * Cache detailed listing
     */
    async cacheDetailedListing(listing, neighborhood) {
        try {
            const cacheData = {
                listing_id: listing.id?.toString(),
                address: listing.address,
                monthly_rent: listing.price,
                bedrooms: listing.bedrooms,
                bathrooms: listing.bathrooms,
                sqft: listing.sqft || 0,
                neighborhood: neighborhood,
                borough: this.getBoroughFromNeighborhood(neighborhood),
                amenities: listing.amenities || [],
                description: listing.description || '',
                market_status: 'pending',
                last_seen_in_search: new Date().toISOString(),
                last_checked: new Date().toISOString(),
                times_seen: 1
            };
            
            const { error } = await this.supabase
                .from('rental_market_cache')
                .upsert(cacheData, { onConflict: 'listing_id' });
            
            if (error) {
                console.warn(`     ⚠️ Failed to cache listing: ${error.message}`);
            }
        } catch (error) {
            console.warn(`     ⚠️ Exception caching listing: ${error.message}`);
        }
    }

    /**
     * Update cache with analysis results
     */
    async updateCacheWithResults(detailedListings, analyzedProperties) {
        try {
            for (const listing of detailedListings) {
                const analysis = analyzedProperties.find(a => a.id === listing.id);
                const status = analysis ? 
                    (analysis.isUndervalued || analysis.isRentStabilized ? 'undervalued' : 'market_rate') : 
                    'market_rate';
                
                await this.supabase
                    .from('rental_market_cache')
                    .update({
                        market_status: status,
                        last_analyzed: new Date().toISOString()
                    })
                    .eq('listing_id', listing.id?.toString());
            }
            
            console.log(`   💾 Updated cache analysis status for ${detailedListings.length} listings`);
        } catch (error) {
            console.warn('⚠️ Error updating cache analysis results:', error.message);
        }
    }

    /**
     * Save analyzed properties using SEPARATE THRESHOLDS for each table
     */
    async saveAnalyzedProperties(properties, neighborhood) {
        console.log(`   💾 Saving ${properties.length} analyzed properties using separate thresholds...`);
        
        let savedToStabilized = 0;
        let savedToUndervalued = 0;
        let skipped = 0;
        
        for (const property of properties) {
            try {
                // SEPARATE THRESHOLD LOGIC
                const isStabilized = property.rentStabilizedProbability >= this.stabilizationThreshold;
                const isUndervalued = property.percentBelowMarket >= this.undervaluationThreshold;
                const stabilizedMeetsThreshold = property.percentBelowMarket >= this.stabilizedUndervaluationThreshold;
                
                if (isStabilized && stabilizedMeetsThreshold) {
                    // Save to rent-stabilized table
                    await this.saveToRentStabilizedTable(property, neighborhood);
                    savedToStabilized++;
                    console.log(`     🔒 STABILIZED: ${property.address} (${property.rentStabilizedProbability}% confidence, ${property.percentBelowMarket.toFixed(1)}% market position)`);
                    
                } else if (!isStabilized && isUndervalued) {
                    // Save to regular undervalued table
                    await this.saveToUndervaluedRentalsTable(property, neighborhood);
                    savedToUndervalued++;
                    console.log(`     💰 UNDERVALUED: ${property.address} (${property.percentBelowMarket.toFixed(1)}% below market)`);
                    
                } else {
                    skipped++;
                    const reason = !isStabilized 
                        ? `Not stabilized (${property.rentStabilizedProbability}% < ${this.stabilizationThreshold}%) or undervalued (${property.percentBelowMarket.toFixed(1)}% < ${this.undervaluationThreshold}%)`
                        : `Stabilized but above threshold (${property.percentBelowMarket.toFixed(1)}% < ${this.stabilizedUndervaluationThreshold}%)`;
                    console.log(`     ⚠️ SKIPPED: ${property.address} - ${reason}`);
                }
                
            } catch (error) {
                console.error(`     ❌ Exception saving ${property.address}: ${error.message}`);
                skipped++;
            }
        }
        
        console.log(`   📊 Save Summary: ${savedToStabilized} stabilized, ${savedToUndervalued} undervalued, ${skipped} skipped`);
    }

    /**
     * Save to undervalued_rent_stabilized table
     */
    async saveToRentStabilizedTable(property, neighborhood) {
        const { error } = await this.supabase
            .from('undervalued_rent_stabilized')
            .upsert({
                listing_id: property.id?.toString(),
                listing_url: property.url || `https://streeteasy.com/rental/${property.id}`,
                address: property.address,
                neighborhood: neighborhood,
                borough: this.getBoroughFromNeighborhood(neighborhood),
                zip_code: property.zipcode,
                
                // Pricing analysis
                monthly_rent: property.price,
                estimated_market_rent: property.estimatedMarketRent || property.price,
                undervaluation_percent: Math.max(0, property.percentBelowMarket),
                potential_monthly_savings: Math.max(0, property.potentialSavings),
                
                // Property details
                bedrooms: property.bedrooms,
                bathrooms: property.bathrooms,
                sqft: property.sqft > 0 ? property.sqft : null,
                description: property.description,
                amenities: property.amenities || [],
                building_amenities: property.buildingAmenities || [],
                building_type: property.buildingType,
                year_built: property.builtIn,
                total_units_in_building: property.totalUnits,
                broker_fee: property.brokerFee,
                available_date: property.availableFrom ? property.availableFrom.split('T')[0] : null,
                lease_term: property.leaseTerms,
                pet_policy: property.petPolicy,
                broker_name: property.brokerName,
                broker_phone: property.brokerPhone,
                
                // Media
                images: property.images || [],
                virtual_tour_url: property.virtualTourUrl,
                floor_plan_url: property.floorPlanUrl,
                
                // RENT STABILIZATION ANALYSIS (Claude) - REQUIRED FIELDS
                rent_stabilized_confidence: property.rentStabilizedProbability,
                rent_stabilized_method: 'claude_ai_analysis',
                rent_stabilization_analysis: {
                    explanation: property.rentStabilizedExplanation || '',
                    key_factors: property.rentStabilizedFactors || [],
                    probability: property.rentStabilizedProbability,
                    legal_indicators: property.rentStabilizedFactors || [],
                    building_criteria: {},
                    dhcr_building_match: this.findRentStabilizedBuildingMatch(property, this.rentStabilizedBuildings),
                    confidence_breakdown: {}
                },
                
                // UNDERVALUATION ANALYSIS (Claude) - REQUIRED FIELDS
                undervaluation_method: 'claude_comparative_analysis',
                undervaluation_confidence: property.undervaluationConfidence,
                comparables_used: Math.max(1, property.comparablesUsed),
                undervaluation_analysis: {
                    adjustments: [],
                    methodology: 'claude_ai_analysis',
                    base_market_rent: property.estimatedMarketRent || property.price,
                    calculation_steps: [property.reasoning || 'Claude AI analysis'],
                    total_adjustments: 0,
                    confidence_factors: {
                        claude_confidence: property.undervaluationConfidence,
                        comparables_quality: property.comparablesUsed
                    },
                    comparable_properties: [],
                    final_market_estimate: property.estimatedMarketRent || property.price
                },
                
                // Scoring and classification
                deal_quality_score: this.calculateDealQualityScore(property),
                market_classification: this.classifyRentStabilizedProperty(property),
                
                // Timestamps
                discovered_at: new Date().toISOString(),
                analyzed_at: new Date().toISOString(),
                analysis_date: new Date().toISOString(),
                display_status: 'active',
                tags: this.generatePropertyTags(property)
            }, {
                onConflict: 'listing_id'
            });
        
        if (error) {
            console.error(`     ❌ Rent-stabilized save error for ${property.address}: ${error.message}`);
            throw error;
        }
    }

    /**
     * Save to undervalued_rentals table (NO rent stabilization fields)
     */
    async saveToUndervaluedRentalsTable(property, neighborhood) {
        const { error } = await this.supabase
            .from('undervalued_rentals')
            .upsert({
                listing_id: property.id?.toString(),
                address: property.address,
                neighborhood: neighborhood,
                borough: this.getBoroughFromNeighborhood(neighborhood),
                zipcode: property.zipcode,
                
                // Pricing analysis (NO rent stabilization fields)
                monthly_rent: property.price,
                discount_percent: property.percentBelowMarket,
                potential_monthly_savings: Math.max(0, property.potentialSavings),
                annual_savings: Math.max(0, property.potentialSavings * 12),
                
                // Property details
                bedrooms: property.bedrooms,
                bathrooms: property.bathrooms,
                sqft: property.sqft || 0,
                property_type: 'apartment',
                available_from: property.availableFrom,
                no_fee: property.noFee || false,
                
                // Building features (derived from amenities)
                doorman_building: this.hasAmenity(property.amenities, ['doorman']),
                elevator_building: this.hasAmenity(property.amenities, ['elevator']),
                pet_friendly: this.hasAmenity(property.amenities, ['pets', 'dogs', 'cats']),
                laundry_available: this.hasAmenity(property.amenities, ['laundry']),
                gym_available: this.hasAmenity(property.amenities, ['gym', 'fitness']),
                rooftop_access: this.hasAmenity(property.amenities, ['roof', 'rooftop']),
                
                built_in: property.builtIn,
                latitude: property.latitude,
                longitude: property.longitude,
                
                // Media and description
                images: property.images || [],
                image_count: (property.images || []).length,
                description: property.description || '',
                amenities: property.amenities || [],
                amenity_count: (property.amenities || []).length,
                
                // Analysis results (NO rent stabilization fields)
                score: this.calculatePropertyScore(property),
                grade: this.calculatePropertyGrade(property),
                reasoning: property.reasoning || '',
                comparison_method: 'claude_ai_analysis',
                reliability_score: property.undervaluationConfidence,
                
                // Status tracking
                last_seen_in_search: new Date().toISOString(),
                analysis_date: new Date().toISOString(),
                status: 'active'
            }, {
                onConflict: 'listing_id'
            });
        
        if (error) {
            console.error(`     ❌ Undervalued rentals save error for ${property.address}: ${error.message}`);
            throw error;
        }
    }

    /**
     * Check if property has specific amenity
     */
    hasAmenity(amenities, searchTerms) {
        if (!Array.isArray(amenities)) return false;
        return searchTerms.some(term => 
            amenities.some(amenity => 
                amenity.toLowerCase().includes(term.toLowerCase())
            )
        );
    }

    /**
     * Calculate property score (0-100)
     */
    calculatePropertyScore(property) {
        let score = 50; // Base score
        
        // Undervaluation bonus
        score += Math.min(30, property.percentBelowMarket);
        
        // Confidence bonus
        score += Math.round(property.undervaluationConfidence * 0.2);
        
        return Math.max(0, Math.min(100, score));
    }

    /**
     * Calculate property grade
     */
    calculatePropertyGrade(property) {
        const score = this.calculatePropertyScore(property);
        if (score >= 90) return 'A+';
        if (score >= 85) return 'A';
        if (score >= 80) return 'B+';
        if (score >= 75) return 'B';
        if (score >= 70) return 'C+';
        if (score >= 65) return 'C';
        if (score >= 60) return 'D';
        return 'F';
    }

    /**
     * Calculate deal quality score for rent-stabilized properties
     */
    calculateDealQualityScore(property) {
        let score = 50;
        
        // Undervaluation bonus
        score += Math.min(25, property.percentBelowMarket * 0.8);
        
        // Rent stabilization confidence bonus
        score += Math.round(property.rentStabilizedProbability * 0.2);
        
        // Undervaluation confidence bonus
        score += Math.round(property.undervaluationConfidence * 0.05);
        
        return Math.max(0, Math.min(100, score));
    }

    /**
     * Classify rent-stabilized property (for the specialized table)
     */
    classifyRentStabilizedProperty(property) {
        if (property.percentBelowMarket >= 20) {
            return 'undervalued';
        } else if (property.percentBelowMarket >= 10) {
            return 'moderately_undervalued';
        } else if (property.percentBelowMarket >= -5) {
            return 'market_rate';
        } else {
            return 'overvalued';
        }
    }

    /**
     * Generate property tags
     */
    generatePropertyTags(property) {
        const tags = [];
        
        if (property.isUndervaluedStabilized) tags.push('goldmine_deal');
        if (property.isUndervalued) tags.push('undervalued');
        if (property.isRentStabilized) tags.push('rent_stabilized');
        if (property.noFee) tags.push('no_fee');
        if (this.hasAmenity(property.amenities, ['doorman'])) tags.push('doorman');
        if (this.hasAmenity(property.amenities, ['elevator'])) tags.push('elevator');
        if (property.percentBelowMarket >= 25) tags.push('exceptional_deal');
        
        return tags;
    }

    /**
     * Find rent-stabilized building match from DHCR data
     */
    findRentStabilizedBuildingMatch(property, rentStabilizedBuildings) {
        if (!rentStabilizedBuildings || rentStabilizedBuildings.length === 0) {
            return false;
        }
        
        const normalizedAddress = this.normalizeAddress(property.address);
        
        const match = rentStabilizedBuildings.find(building => {
            const buildingAddress = this.normalizeAddress(building.address || '');
            return buildingAddress && normalizedAddress.includes(buildingAddress);
        });
        
        return !!match;
    }

    /**
     * Normalize address for matching
     */
    normalizeAddress(address) {
        if (!address) return '';
        return address.toLowerCase()
            .replace(/[^\w\s]/g, ' ')
            .replace(/\s+/g, ' ')
            .trim();
    }

    /**
     * Get borough from neighborhood
     */
    getBoroughFromNeighborhood(neighborhood) {
        for (const [borough, neighborhoods] of Object.entries(this.boroughMap)) {
            if (neighborhoods.includes(neighborhood)) {
                return borough;
            }
        }
        return 'manhattan'; // Default fallback
    }

    /**
     * Get analysis summary from CORRECT database tables
     */
    async getAnalysisSummary() {
        try {
            // Get undervalued rentals from the general table
            const { data: undervalued, error: undervaluedError } = await this.supabase
                .from('undervalued_rentals')
                .select('*')
                .eq('status', 'active')
                .gte('discount_percent', this.undervaluationThreshold)
                .order('discount_percent', { ascending: false })
                .limit(10);

            // Get rent-stabilized properties from the specialized table
            const { data: stabilized, error: stabilizedError } = await this.supabase
                .from('undervalued_rent_stabilized')
                .select('*')
                .eq('display_status', 'active')
                .gte('rent_stabilized_confidence', this.stabilizationThreshold)
                .order('rent_stabilized_confidence', { ascending: false })
                .limit(10);

            // Get GOLDMINE DEALS - both undervalued AND rent-stabilized
            const { data: goldmine, error: goldmineError } = await this.supabase
                .from('undervalued_rent_stabilized')
                .select('*')
                .eq('display_status', 'active')
                .gte('undervaluation_percent', this.undervaluationThreshold)
                .gte('rent_stabilized_confidence', this.stabilizationThreshold)
                .order('deal_quality_score', { ascending: false })
                .limit(5);

            return {
                topUndervalued: undervalued || [],
                topStabilized: stabilized || [],
                goldmineDeals: goldmine || [] // Both undervalued AND stabilized - THE BEST DEALS
            };

        } catch (error) {
            console.error('Error getting summary:', error.message);
            return { topUndervalued: [], topStabilized: [], goldmineDeals: [] };
        }
    }

    /**
     * Setup database (create tables if needed)
     */
    async setupDatabase() {
        console.log('🔧 Setting up database...');
        
        try {
            // Test connection
            const { data, error } = await this.supabase
                .from('rental_market_cache')
                .select('id')
                .limit(1);
                
            if (error && error.message.includes('does not exist')) {
                console.log('⚠️ Database tables missing - please run the SQL schema');
                console.log('📋 Check the CURRENT SUPABASE SQL in your project files');
                return false;
            }
            
            console.log('✅ Database connection verified');
            return true;
            
        } catch (error) {
            console.error('❌ Database setup failed:', error.message);
            return false;
        }
    }

    /**
     * Cleanup old data
     */
    async cleanupOldData() {
        console.log('🧹 Cleaning up old data...');
        
        try {
            const oneWeekAgo = new Date();
            oneWeekAgo.setDate(oneWeekAgo.getDate() - 7);
            
            // Mark old cache entries as stale
            const { error: cacheError } = await this.supabase
                .from('rental_market_cache')
                .update({ market_status: 'likely_rented' })
                .lt('last_seen_in_search', oneWeekAgo.toISOString())
                .eq('market_status', 'pending');
            
            if (cacheError) {
                console.warn('⚠️ Cache cleanup warning:', cacheError.message);
            } else {
                console.log('✅ Cache cleanup completed');
            }
            
        } catch (error) {
            console.error('❌ Cleanup failed:', error.message);
        }
    }

    /**
     * Utility delay function
     */
    async delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    /**
     * Get usage statistics
     */
    getUsageStats() {
        return {
            apiCallsUsed: this.apiCallsUsed,
            rateLimitHits: this.rateLimitHits,
            estimatedCost: this.apiCallsUsed * 0.0006
        };
    }
}

// Command line interface
async function main() {
    const system = new ClaudePoweredRentalSystem();
    const args = process.argv.slice(2);

    if (args.includes('--help')) {
        console.log('🤖 Claude-Powered Rental Analysis System');
        console.log('');
        console.log('Commands:');
        console.log('  node claude-powered-rentals-system.js              # Run full analysis');
        console.log('  node claude-powered-rentals-system.js --summary    # Show top deals');
        console.log('  node claude-powered-rentals-system.js --setup      # Setup database');
        console.log('  node claude-powered-rentals-system.js --cleanup    # Cleanup old data');
        console.log('  node claude-powered-rentals-system.js --test       # Test single neighborhood');
        console.log('  node claude-powered-rentals-system.js --help       # Show this help');
        console.log('');
        console.log('Environment Variables:');
        console.log('  TEST_NEIGHBORHOOD=soho                             # Test single neighborhood');
        console.log('  UNDERVALUATION_THRESHOLD=15                        # Regular undervaluation threshold');
        console.log('  STABILIZED_UNDERVALUATION_THRESHOLD=-100           # Stabilized threshold');
        console.log('  RENT_STABILIZED_CONFIDENCE_THRESHOLD=40            # Stabilization confidence');
        console.log('  CLAUDE_API_KEY=your_key                            # Claude API key');
        console.log('');
        console.log('Examples:');
        console.log('  TEST_NEIGHBORHOOD=soho npm run claude-test         # Test SoHo only');
        console.log('  npm run claude-test -- --summary                   # Show summary');
        return;
    }

    if (args.includes('--setup')) {
        console.log('🔧 Setting up Claude-powered rental system...');
        const success = await system.setupDatabase();
        if (success) {
            console.log('✅ Setup completed successfully');
        } else {
            console.log('❌ Setup failed - check database schema');
        }
        return;
    }

    if (args.includes('--cleanup')) {
        console.log('🧹 Cleaning up old data...');
        await system.cleanupOldData();
        console.log('✅ Cleanup completed');
        return;
    }

    if (args.includes('--test')) {
        console.log('🧪 Testing Claude analysis system...');
        if (!system.testNeighborhood) {
            console.log('⚠️ No TEST_NEIGHBORHOOD set - using SoHo');
            system.testNeighborhood = 'soho';
        }
        console.log(`🎯 Testing neighborhood: ${system.testNeighborhood}`);
        
        const results = await system.runComprehensiveRentalAnalysis();
        
        const stats = system.getUsageStats();
        console.log('\n📊 TEST RESULTS:');
        console.log(`   🤖 Claude API calls: ${stats.apiCallsUsed}`);
        console.log(`   💰 Estimated cost: ${stats.estimatedCost.toFixed(4)}`);
        console.log(`   📈 Rate limit hits: ${stats.rateLimitHits}`);
        console.log(`   🏠 Properties analyzed: ${results.totalAnalyzed}`);
        console.log(`   💎 Goldmine deals: ${results.undervaluedStabilized}`);
        
        return;
    }

    if (args.includes('--summary')) {
        console.log('📊 ANALYSIS SUMMARY');
        console.log('=' .repeat(50));
        
        const summary = await system.getAnalysisSummary();
        
        console.log('\n💎 GOLDMINE DEALS (Undervalued + Rent Stabilized):');
        if (summary.goldmineDeals.length === 0) {
            console.log('   No goldmine deals found. Run analysis first.');
        } else {
            summary.goldmineDeals.forEach((deal, i) => {
                console.log(`${i + 1}. ${deal.address} - ${deal.monthly_rent}/month (${deal.undervaluation_percent?.toFixed(1)}% below market, ${deal.rent_stabilized_confidence}% stabilized)`);
            });
        }
        
        console.log('\n🏠 TOP UNDERVALUED RENTALS:');
        if (summary.topUndervalued.length === 0) {
            console.log('   No undervalued rentals found. Run analysis first.');
        } else {
            summary.topUndervalued.slice(0, 5).forEach((deal, i) => {
                console.log(`${i + 1}. ${deal.address} - ${deal.monthly_rent}/month (${deal.discount_percent?.toFixed(1)}% below market)`);
            });
        }
        
        console.log('\n🔒 TOP RENT-STABILIZED PROPERTIES:');
        if (summary.topStabilized.length === 0) {
            console.log('   No stabilized properties found. Run analysis first.');
        } else {
            summary.topStabilized.slice(0, 5).forEach((deal, i) => {
                console.log(`${i + 1}. ${deal.address} - ${deal.monthly_rent}/month (${deal.rent_stabilized_confidence}% confidence)`);
            });
        }
        
        return;
    }

    // Default: Run full analysis
    try {
        console.log('🚀 Starting Claude-powered rental analysis...');
        
        const results = await system.runComprehensiveRentalAnalysis();
        
        if (results.undervaluedStabilized > 0) {
            console.log('\n💎 Use --summary to see your best deals!');
            console.log('💡 Example: node claude-powered-rentals-system.js --summary');
        }
        
        const stats = system.getUsageStats();
        console.log('\n📊 FINAL STATS:');
        console.log(`   🤖 Total API calls: ${stats.apiCallsUsed}`);
        console.log(`   💰 Total cost: ${stats.estimatedCost.toFixed(3)}`);
        console.log(`   📈 Rate limit hits: ${stats.rateLimitHits}`);
        console.log(`   ⚡ Cache efficiency: ${results.cacheEfficiency}%`);
        
    } catch (error) {
        console.error('💥 Analysis failed:', error.message);
        console.error('💡 Try running with --test first to check configuration');
        process.exit(1);
    }
}

// Export for use in Railway and other modules
module.exports = ClaudePoweredRentalSystem;

// Run if called directly
if (require.main === module) {
    main().catch(error => {
        console.error('💥 Claude-powered rental system crashed:', error);
        process.exit(1);
    });
}
