// claude-powered-rentals-system.js
// FIXED VERSION - Comprehensive rent stabilization detection with Claude AI analysis
// FIXES:
// 1. Removed non-existent net.http_post calls (causing save failures)
// 2. Fixed rent_stabilized_method constraint violations 
// 3. Enhanced JSON parsing error handling
// 4. Fixed targetProperty reference errors
// 5. Added smart caching to reduce API calls
// 6. Fixed fetchActiveListings syntax
// 7. RAILWAY FIX: Corrected enhanced method mapping logic
// 8. RAILWAY FIX: Actually use mapEnhancedMethodToDbConstraint function
// 9. RAILWAY FIX: Proper error handling for enhanced analysis

require('dotenv').config();
const axios = require('axios');
const { createClient } = require('@supabase/supabase-js');
const EnhancedClaudeMarketAnalyzer = require('./claude-market-analyzer.js');

class ClaudePoweredRentalsSystem {
    constructor() {
        // Supabase configuration
        this.supabase = createClient(
            process.env.SUPABASE_URL,
              process.env.SUPABASE_SERVICE_ROLE_KEY
        );
        
        // API configuration - FIXED: Remove non-existent rapid API
        this.rapidApiKey = process.env.RAPIDAPI_KEY;
        this.claudeAnalyzer = new EnhancedClaudeMarketAnalyzer();
        
        // Analysis thresholds
        this.stabilizationThreshold = parseInt(process.env.RENT_STABILIZED_CONFIDENCE_THRESHOLD) || 60;
        this.undervaluationThreshold = parseInt(process.env.UNDERVALUATION_THRESHOLD) || 15;
        this.stabilizedUndervaluationThreshold = parseInt(process.env.STABILIZED_UNDERVALUATION_THRESHOLD) || -100;
        this.maxListingsPerNeighborhood = parseInt(process.env.MAX_LISTINGS_PER_NEIGHBORHOOD) || 500;
        
        // Cache settings
        this.cacheDuration = parseInt(process.env.CACHE_DURATION_DAYS) || 7;
        
        // Statistics tracking
        this.apiCallsUsed = 0;
        this.totalAnalyzed = 0;
        this.undervaluedCount = 0;
        this.stabilizedCount = 0;
        this.undervaluedStabilizedCount = 0;
        
        console.log('🏠 Claude-Powered Rent Stabilization System initialized');
        console.log(`   🎯 Stabilization threshold: ${this.stabilizationThreshold}%`);
        console.log(`   💰 Undervaluation threshold: ${this.undervaluationThreshold}%`);
        console.log(`   🔒 Stabilized undervaluation threshold: ${this.stabilizedUndervaluationThreshold}%`);
    }

  /**
     * NEW: Run rented detection for undervalued_rent_stabilized listings in specific neighborhood
     * MATCHES THE EXACT SAME FUNCTIONALITY AS biweekly-streeteasy-rentals.js
     */
    async runRentedDetectionForNeighborhood(searchResults, neighborhood) {
        const currentTime = new Date().toISOString();
        const currentListingIds = searchResults.map(r => r.id?.toString()).filter(Boolean);
        
        try {
            const threeDaysAgo = new Date();
            threeDaysAgo.setDate(threeDaysAgo.getDate() - 3);

            // Get rent-stabilized listings in this neighborhood that weren't in current search
            const { data: missingRentStabilized, error: missingError } = await this.supabase
                .from('undervalued_rent_stabilized')
                .select('listing_id')
                .eq('neighborhood', neighborhood)
                .eq('display_status', 'active')
                .not('listing_id', 'in', `(${currentListingIds.map(id => `"${id}"`).join(',')})`)
                .lt('last_seen_in_search', threeDaysAgo.toISOString());

            if (missingError) {
                console.warn('⚠️ Error checking for missing rent-stabilized listings:', missingError.message);
                return { markedRented: 0 };
            }

            // Mark corresponding entries in undervalued_rent_stabilized as likely rented
            let markedRented = 0;
            if (missingRentStabilized && missingRentStabilized.length > 0) {
                const missingIds = missingRentStabilized.map(r => r.listing_id);
                
                const { error: markRentedError } = await this.supabase
                    .from('undervalued_rent_stabilized')
                    .update({
                        display_status: 'rented',
                        likely_rented: true,
                        rented_detected_at: currentTime
                    })
                    .in('listing_id', missingIds)
                    .eq('display_status', 'active');

                if (!markRentedError) {
                    markedRented = missingIds.length;
                    console.log(`   🏠 Marked ${markedRented} rent-stabilized listings as likely rented (not seen in recent search)`);
                } else {
                    console.warn('⚠️ Error marking rent-stabilized listings as rented:', markRentedError.message);
                }
            }

            return { markedRented };
        } catch (error) {
            console.warn('⚠️ Error in rent-stabilized rented detection for neighborhood:', error.message);
            return { markedRented: 0 };
        }
    }

    /**
     * NEW: Update last_seen_in_search timestamps for rent-stabilized listings
     * MATCHES THE EXACT SAME FUNCTIONALITY AS biweekly-streeteasy-rentals.js
     */
    async updateRentStabilizedTimestamps(searchResults, neighborhood) {
        const currentTime = new Date().toISOString();
        
        try {
            // Update search timestamps for listings we found
            for (const listing of searchResults) {
                if (!listing.id) continue;
                
                try {
                    const { error } = await this.supabase
                        .from('undervalued_rent_stabilized')
                        .update({
                            last_seen_in_search: currentTime,
                            times_seen_in_search: 1  // Could be improved with increment logic
                        })
                        .eq('listing_id', listing.id.toString());

                    if (error && !error.message.includes('0 rows')) {
                        console.warn(`⚠️ Error updating search timestamp for rent-stabilized ${listing.id}:`, error.message);
                    }
                } catch (itemError) {
                    console.warn(`⚠️ Error processing search timestamp for rent-stabilized ${listing.id}:`, itemError.message);
                }
            }

            // Run rented detection for this neighborhood
            const { markedRented } = await this.runRentedDetectionForNeighborhood(searchResults, neighborhood);
            
            console.log(`   💾 Updated rent-stabilized search timestamps: ${searchResults.length} listings, marked ${markedRented} as rented`);
            return { updated: searchResults.length, markedRented };

        } catch (error) {
            console.warn('⚠️ Error updating rent-stabilized search timestamps:', error.message);
            return { updated: 0, markedRented: 0 };
        }
    }

    /**
     * MAIN ANALYSIS FUNCTION - Analyze neighborhood for rent-stabilized opportunities
     */
    async analyzeNeighborhoodForRentStabilized(neighborhood, options = {}) {
    console.log(`\n🏘️ Analyzing ${neighborhood} for rent-stabilized opportunities...`);
    
    const results = {
    neighborhood,
    totalListings: 0,
    totalAnalyzed: 0,
    undervaluedCount: 0,
    stabilizedCount: 0,
    undervaluedStabilizedCount: 0,
    savedCount: 0,
    skippedCount: 0,
    errors: [],
    rentedDetection: { markedRented: 0, updated: 0 }
};
    
    try {
        // FIXED STEP 1: Fetch current active listings from API first
        const activeListings = await this.fetchActiveListings(neighborhood);
        console.log(`   🔍 Found ${activeListings.length} active listings`);
        
        if (activeListings.length === 0) {
            console.log(`   ⚠️ No active listings found for ${neighborhood}`);
            return results;
        }
        
        // FIXED STEP 2: Get cached listings for comparison
        const cachedListings = await this.getCachedNeighborhoodListings(neighborhood);
        console.log(`   📦 Found ${cachedListings.length} cached listings`);
        
        // FIXED STEP 3: Smart comparison - only analyze new/changed listings
        const { needFetch, priceUpdates, cacheHits } = await this.getListingsNeedingFetch(activeListings, cachedListings);
        console.log(`   🎯 Analysis needed: ${needFetch.length}, Cache hits: ${cacheHits}, Price changes: ${priceUpdates.length}`);
        
        // FIXED STEP 4: Mark missing listings as rented
        await this.markMissingListingsAsRented(activeListings.map(l => l.id), neighborhood);
        
        results.totalListings = activeListings.length;

// NEW: Update timestamps and run rented detection for rent-stabilized listings
console.log(`   🔍 Running rented detection for ${neighborhood}...`);
results.rentedDetection = await this.updateRentStabilizedTimestamps(activeListings, neighborhood);
        
       // STEP 5: Fetch detailed listing data FIRST, then analyze with Claude
console.log(`   🔍 Fetching detailed data for ${needFetch.length} properties...`);
const detailedListings = await this.fetchDetailedListingsWithCache(needFetch.slice(0, this.maxListingsPerNeighborhood), neighborhood);

console.log(`   ✅ Got detailed data for ${detailedListings.length} properties`);

// STEP 6: Now analyze with Claude using COMPLETE data (with real addresses)
const analyzedProperties = [];

// ADD THIS: Calculate dynamic threshold for this neighborhood
const totalListings = results.totalListings || detailedListings.length;
const dynamicThreshold = this.calculateDynamicThreshold(totalListings, neighborhood);

for (const listing of detailedListings) {
    try {
        console.log(`🤖 Enhanced Claude analyzing rental: ${listing.address}`);
        
        // Skip if still no address after detailed fetch
        if (!listing.address || listing.address === 'Address not available') {
            console.log(`     ⚠️ SKIPPED: Still no address after detailed fetch for ${listing.id}`);
            continue;
        }
        
        // Get rent stabilization data for context
        const rentStabilizedBuildings = options.rentStabilizedBuildings || [];
        
        // Call Claude for comprehensive analysis
        const analysis = await this.claudeAnalyzer.analyzeRentalsUndervaluation(
            listing,
            detailedListings, // Use detailed listings as comparables (not activeListings)
            neighborhood,
            { 
                undervaluationThreshold: dynamicThreshold, // ← This will pass 10 or 15
                rentStabilizedBuildings
            }
        );
        
        if (analysis && analysis.confidence > 0) {
            // FIXED: Convert data types at source
            const cleanAnalysis = this.cleanAnalysisData(analysis);
            
            // Determine classifications
            const isUndervalued = cleanAnalysis.percentBelowMarket >= this.undervaluationThreshold;
            const isStabilized = cleanAnalysis.rentStabilizedProbability >= this.stabilizationThreshold;
            
            const analyzedProperty = {
                ...listing,
                
                // Market analysis results - FIXED: Proper data types
                percentBelowMarket: cleanAnalysis.percentBelowMarket,
                estimatedMarketRent: cleanAnalysis.estimatedMarketRent || listing.price,
                potentialSavings: cleanAnalysis.potentialSavings,
                undervaluationConfidence: cleanAnalysis.undervaluationConfidence,
                
                // Rent stabilization analysis - FIXED: Proper data types
                rentStabilizedProbability: cleanAnalysis.rentStabilizedProbability,
                rentStabilizedFactors: cleanAnalysis.rentStabilizedFactors,
                rentStabilizedExplanation: cleanAnalysis.rentStabilizedExplanation,
                
                // RAILWAY FIX: Add enhanced analysis data structure
                enhancedRentStabilization: analysis.enhancedRentStabilization || null,
                enhancedUndervaluation: analysis.enhancedUndervaluation || null,
                
                // Classifications
                isUndervalued: isUndervalued,
                isRentStabilized: isStabilized,
                isUndervaluedStabilized: isUndervalued && isStabilized,
                
                // Analysis metadata
                analysisMethod: 'claude_ai',
                reasoning: cleanAnalysis.reasoning,
                comparablesUsed: detailedListings.length,
                fromCache: false
            };
            
            analyzedProperties.push(analyzedProperty);
            
            // Cache the detailed listing and analysis
            await this.cacheDetailedListing(analyzedProperty, neighborhood);
            
            // Update counts
            if (isUndervalued) results.undervaluedCount++;
            if (isStabilized) results.stabilizedCount++;
            if (isUndervalued && isStabilized) results.undervaluedStabilizedCount++;
            
            console.log(`     ✅ ${listing.address}: ${cleanAnalysis.percentBelowMarket}% below market, ${cleanAnalysis.rentStabilizedProbability}% stabilized`);
        } else {
            console.log(`     ⚠️ Analysis failed for ${listing.address}: ${analysis?.error || 'Unknown error'}`);
        }
        
    } catch (error) {
        console.warn(`     ⚠️ Analysis exception for ${listing.address}: ${error.message}`);
        results.errors.push(`${listing.address}: ${error.message}`);
    }
    
    results.totalAnalyzed++;
    
    // Rate limiting between properties
    await this.delay(100);
}
        
        // FIXED STEP 6: Save results using separate thresholds
        if (analyzedProperties.length > 0) {
            await this.saveAnalyzedProperties(analyzedProperties, neighborhood, results);
        }
        
        return results;
        
    } catch (error) {
        console.error(`   ❌ Neighborhood analysis failed: ${error.message}`);
        results.errors.push(`Neighborhood analysis: ${error.message}`);
        throw error;
    }
}

    /**
     * Get cached neighborhood listings from rental_market_cache
     */
    async getCachedNeighborhoodListings(neighborhood) {
        try {
            const { data, error } = await this.supabase
                .from('rental_market_cache')
                .select('*')
                .eq('neighborhood', neighborhood)
                .neq('market_status', 'fetch_failed')
                .not('address', 'is', null)  // Only get listings with complete details
                .order('last_seen_in_search', { ascending: false })
                .limit(this.maxListingsPerNeighborhood);
            
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
                description: row.description || '',
                noFee: row.no_fee || false,
                zipcode: row.zipcode,
                builtIn: row.built_in,
                propertyType: row.property_type || 'apartment'
            }));
        } catch (error) {
            console.warn(`   ⚠️ Failed to get cached listings: ${error.message}`);
            return [];
        }
    }

/**
 * FIXED: Fetch active listings from StreetEasy API using correct parameters
 */
async fetchActiveListings(neighborhood) {
    try {
        console.log(`   🔍 Fetching active listings for ${neighborhood}...`);
        
        let allRentals = [];
        let offset = 0;
        const limit = Math.min(500, this.maxListingsPerNeighborhood); // API limit is 500
        const maxListings = this.maxListingsPerNeighborhood || 2000;
        let hasMoreData = true;
        
        while (hasMoreData && allRentals.length < maxListings) {
            const response = await axios.get('https://streeteasy-api.p.rapidapi.com/rentals/search', {
                params: {
                    areas: neighborhood,  // FIXED: Use "areas" not "neighborhood"
                    limit: limit,
                    minPrice: 1000,
                    maxPrice: 20000,
                    offset: offset  // ✅ ONLY CHANGE: Now properly increments!
                },
                headers: {
                    'X-RapidAPI-Key': this.rapidApiKey,
                    'X-RapidAPI-Host': 'streeteasy-api.p.rapidapi.com'
                },
                timeout: 30000
            });
            
            this.apiCallsUsed++;
            
            // Handle response structure (from working biweekly code)
            let rentalData = [];
            if (response.data) {
                if (response.data.results && Array.isArray(response.data.results)) {
                    rentalData = response.data.results;
                } else if (response.data.listings && Array.isArray(response.data.listings)) {
                    rentalData = response.data.listings;
                } else if (response.data.rentals && Array.isArray(response.data.rentals)) {
                    rentalData = response.data.rentals;
                } else if (Array.isArray(response.data)) {
                    rentalData = response.data;
                }
            }
            
            // Add to total
            allRentals = allRentals.concat(rentalData);
            
            // Check if we should continue
            if (rentalData.length < limit) {
                // Got fewer than limit, means we've reached the end
                hasMoreData = false;
            } else if (allRentals.length >= maxListings) {
                // Hit our per-neighborhood limit
                hasMoreData = false;
            } else {
                // Increment offset for next batch
                offset += limit;
            }
        }
        
        console.log(`   ✅ Found ${allRentals.length} active listings`);
        
        return allRentals.map(listing => ({
            id: listing.id?.toString(),
            address: listing.address || 'Address not available',
            price: listing.price || listing.monthlyRent || 0,
            bedrooms: listing.bedrooms || 0,
            bathrooms: listing.bathrooms || 0,
            sqft: listing.sqft || 0,
            neighborhood: neighborhood,
            amenities: listing.amenities || [],
            description: listing.description || '',
            noFee: listing.noFee || false,
            zipcode: listing.zipcode,
            builtIn: listing.builtIn,
            propertyType: listing.propertyType || 'apartment',
            url: listing.url || `https://streeteasy.com/rental/${listing.id}`
        }));
        
    } catch (error) {
        console.warn(`   ⚠️ Failed to fetch active listings: ${error.message}`);
        if (error.response) {
            console.warn(`   📊 Response status: ${error.response.status}`);
            console.warn(`   📊 Response data:`, error.response.data);
        }
        return [];
    }
}

/**
 * FIXED: Clean Claude analysis data and convert to proper types
 */
cleanAnalysisData(analysis) {
    return {
        percentBelowMarket: this.safeDecimal(analysis.percentBelowMarket, 1, 0),
        estimatedMarketRent: this.safeInt(analysis.estimatedMarketRent, 0),
        potentialSavings: this.safeInt(analysis.potentialSavings, 0),
        undervaluationConfidence: this.safeInt(analysis.confidence || analysis.undervaluationConfidence, 0),
        rentStabilizedProbability: this.safeInt(analysis.rentStabilizedProbability, 0),
        rentStabilizedFactors: Array.isArray(analysis.rentStabilizedFactors) ? analysis.rentStabilizedFactors : [],
        rentStabilizedExplanation: analysis.rentStabilizedExplanation || 'No stabilization indicators found',
        reasoning: analysis.reasoning || 'Claude AI market analysis'
    };
}

    /**
     * Fetch detailed property information with caching
     * RESTORED: This function was referenced but missing
     */
    async fetchDetailedListingsWithCache(listings, neighborhood) {
        const detailed = [];
        
        for (const listing of listings) {
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
     * Cache detailed listing information
     * RESTORED: This function was referenced but missing
     */
    async cacheDetailedListing(listing, neighborhood) {
        // ONLY CACHE IF FULLY DETAILED:
        if (!listing.address || listing.bedrooms === undefined) {
            console.warn(`     ⚠️ Not caching incomplete listing ${listing.id}`);
            return;
        }
        
        try {
            const cacheData = {
                listing_id: listing.id?.toString(),
                address: listing.address,  // Now guaranteed to exist
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
     * RESTORED: Smart caching - check which listings need individual API calls
     */
    async getListingsNeedingFetch(activeListings, cachedListings) {
        const cachedIds = new Set(cachedListings.map(c => c.id));
        const needFetch = [];
        const priceUpdates = [];
        
        for (const listing of activeListings) {
            const cached = cachedListings.find(c => c.id === listing.id);
            
            if (!cached) {
                // New listing - needs full fetch
                needFetch.push(listing);
            } else if (cached.price !== listing.price) {
                // Price changed - needs re-fetch and analysis update
                console.log(`   💰 Price change detected for ${listing.id}: ${cached.price} → ${listing.price}`);
                needFetch.push(listing);
                priceUpdates.push({ id: listing.id, oldPrice: cached.price, newPrice: listing.price });
            }
            // else: cached and price same - no fetch needed
        }
        
        return { needFetch, priceUpdates, cacheHits: activeListings.length - needFetch.length };
    }

// ✅ NEW VERSION (Gets ALL cached listings with pagination):
async markMissingListingsAsRented(currentListingIds, neighborhood) {
    try {
        const currentIds = new Set(currentListingIds.map(id => id.toString()));
        
        // ✅ FIX: Get ALL cached listings with pagination
        let allCachedListings = [];
        let offset = 0;
        const limit = 1000; // Supabase batch size
        let hasMoreData = true;
        
        while (hasMoreData) {
            const { data, error } = await this.supabase
                .from('rental_market_cache')
                .select('listing_id')
                .eq('neighborhood', neighborhood)
                .not('address', 'is', null)
                .neq('market_status', 'likely_rented')
                .range(offset, offset + limit - 1);
            
            if (error) throw error;
            
            if (!data || data.length === 0) {
                hasMoreData = false;
            } else {
                allCachedListings = allCachedListings.concat(data);
                if (data.length < limit) hasMoreData = false;
                offset += limit;
            }
        }
        
        const missingIds = allCachedListings
            .map(row => row.listing_id)
            .filter(id => !currentIds.has(id));
        
        // ... rest of function stays exactly the same
        if (missingIds.length > 0) {
            // Mark as likely rented in cache
            const { error: updateError } = await this.supabase
                .from('rental_market_cache')
                .update({ 
                    market_status: 'likely_rented',
                    last_checked: new Date().toISOString()
                })
                .in('listing_id', missingIds);
            
            if (updateError) throw updateError;

            // Mark corresponding entries in undervalued_rentals as likely rented
            const { error: markRentalsError } = await this.supabase
                .from('undervalued_rentals')
                .update({
                    status: 'likely_rented',
                    likely_rented: true,
                    rented_detected_at: new Date().toISOString()
                })
                .in('listing_id', missingIds)
                .eq('status', 'active');
            
            console.log(`   🔄 Marked ${missingIds.length} missing listings as likely rented`);
            
            if (markRentalsError) {
                console.warn('⚠️ Error marking undervalued_rentals as rented:', markRentalsError.message);
            }
        }
        
    } catch (error) {
        console.warn(`   ⚠️ Error marking missing listings: ${error.message}`);
    }
}

    /**
     * Remove likely rented properties from analysis tables
     */
    async removeRentedFromAnalysisTables(listingIds) {
        try {
            // Remove from both tables - simpler than updating status
            await Promise.all([
                this.supabase.from('undervalued_rentals').delete().in('listing_id', listingIds),
                this.supabase.from('undervalued_rent_stabilized').delete().in('listing_id', listingIds)
            ]);
            
            console.log(`   🗑️ Removed ${listingIds.length} rented properties from analysis tables`);
        } catch (error) {
            console.warn(`   ⚠️ Error removing rented properties: ${error.message}`);
        }
    }

    /**
     * Handle price change updates (re-analyze and update tables)
     */
    async handlePriceChangeUpdates(priceUpdates, analyzedProperties) {
        for (const update of priceUpdates) {
            try {
                // Find the re-analyzed property
                const property = analyzedProperties.find(p => p.id === update.id);
                if (!property) continue;
                
                console.log(`   🔄 Updating analysis for ${property.address} (price: ${update.oldPrice} → ${update.newPrice})`);
                
                // Update cache with new price
                await this.supabase
                    .from('rental_market_cache')
                    .update({ 
                        monthly_rent: update.newPrice,
                        last_checked: new Date().toISOString() 
                    })
                    .eq('listing_id', update.id);
                
                // Remove old analysis (if exists) and re-save with new analysis
                await Promise.all([
                    this.supabase.from('undervalued_rentals').delete().eq('listing_id', update.id),
                    this.supabase.from('undervalued_rent_stabilized').delete().eq('listing_id', update.id)
                ]);
                
                // Re-save will happen in normal save flow with updated analysis
                
            } catch (error) {
                console.warn(`   ⚠️ Error handling price update for ${update.id}: ${error.message}`);
            }
        }
    }

    /**
 * Calculate dynamic undervaluation threshold based on neighborhood size
 * Small neighborhoods get lower thresholds to ensure variety
 */
calculateDynamicThreshold(activeListingsCount, neighborhood) {
    const baseThreshold = this.undervaluationThreshold; // 15% from env
    const lowInventoryThreshold = 10; // Lower threshold for small neighborhoods
    const inventoryBreakpoint = 200; // Listings count breakpoint
    
    if (activeListingsCount < inventoryBreakpoint) {
        console.log(`   📊 Small neighborhood (${activeListingsCount} listings) - Using ${lowInventoryThreshold}% threshold for variety`);
        return lowInventoryThreshold;
    } else {
        console.log(`   📊 Large neighborhood (${activeListingsCount} listings) - Using ${baseThreshold}% threshold for quality`);
        return baseThreshold;
    }
}

/**
 * FIXED: Save analyzed properties using SEPARATE THRESHOLDS for each table
 * UPDATED: Now uses dynamic threshold based on neighborhood size
 */
async saveAnalyzedProperties(properties, neighborhood, results) {
    // Calculate dynamic threshold based on neighborhood size
    const totalListings = results.totalListings || properties.length;
    const dynamicUndervaluationThreshold = this.calculateDynamicThreshold(totalListings, neighborhood);
    
    console.log(`   💾 Saving ${properties.length} analyzed properties using dynamic thresholds...`);
    console.log(`   📊 Using ${dynamicUndervaluationThreshold}% undervaluation threshold for this neighborhood`);
    
    let savedToStabilized = 0;
    let savedToUndervalued = 0;
    let skipped = 0;
    
    for (const property of properties) {
        try {
            // DYNAMIC THRESHOLD LOGIC - Use calculated threshold instead of fixed
            const isStabilized = property.rentStabilizedProbability >= this.stabilizationThreshold;
            const isUndervalued = property.percentBelowMarket >= dynamicUndervaluationThreshold; // CHANGED: Use dynamic
            const stabilizedMeetsThreshold = property.percentBelowMarket >= this.stabilizedUndervaluationThreshold;
            
            if (isStabilized && stabilizedMeetsThreshold) {
                // Save to rent-stabilized table with FIXED constraint handling
                await this.saveToRentStabilizedTable(property, neighborhood);
                savedToStabilized++;
                console.log(`     🔒 STABILIZED: ${property.address} (${property.rentStabilizedProbability}% confidence, ${property.percentBelowMarket?.toFixed(1)}% market position)`);
                
            } else if (!isStabilized && isUndervalued) {
                // Save to regular undervalued table - FIXED: Use Supabase instead of net.http_post
                await this.saveToUndervaluedRentalsTable(property, neighborhood);
                savedToUndervalued++;
                console.log(`     💰 UNDERVALUED: ${property.address} (${property.percentBelowMarket?.toFixed(1)}% below market)`);
                
            } else {
                skipped++;
                const reason = !isStabilized 
                    ? `Not stabilized (${property.rentStabilizedProbability}% < ${this.stabilizationThreshold}%) or undervalued (${property.percentBelowMarket?.toFixed(1)}% < ${dynamicUndervaluationThreshold}%)`
                    : `Stabilized but above threshold (${property.percentBelowMarket?.toFixed(1)}% < ${this.stabilizedUndervaluationThreshold}%)`;
                console.log(`     ⚠️ SKIPPED: ${property.address} - ${reason}`);
            }
            
        } catch (error) {
            console.error(`     ❌ Exception saving ${property.address}: ${error.message}`);
            skipped++;
        }
    }
    
    console.log(`   📊 Save Summary: ${savedToStabilized} stabilized, ${savedToUndervalued} undervalued, ${skipped} skipped`);
    
    // Update results
    results.savedCount = savedToStabilized + savedToUndervalued;
    results.skippedCount = skipped;
}
    /**
     * RAILWAY FIX: Enhanced method mapping function
     */
    mapEnhancedMethodToDbConstraint(enhancedMethod) {
        const methodMappings = {
            // Rent stabilization methods
            'dhcr_database_verification': 'dhcr_registered',
            'age_and_size_analysis': 'building_analysis', 
            'market_rent_analysis': 'circumstantial',
            'building_characteristics_analysis': 'building_analysis',
            'comprehensive_analysis': 'circumstantial',
            
            // Undervaluation methods
            'exact_bed_bath_amenity_match': 'exact_bed_bath_amenity_match',
            'bed_bath_specific_pricing': 'bed_bath_specific_pricing',
            'bed_specific_with_adjustments': 'bed_specific_with_adjustments',
            'price_per_sqft_fallback': 'price_per_sqft_fallback',
            'comparative_market_analysis': 'price_per_sqft_fallback'
        };
        
        return methodMappings[enhancedMethod] || 'circumstantial';
    }

    /**
     * RAILWAY FIX: Save to undervalued_rent_stabilized table with enhanced analysis
     */
    async saveToRentStabilizedTable(property, neighborhood) {
        try {
            // RAILWAY FIX: Use enhanced analysis data if available
            const enhancedRentStabilization = property.enhancedRentStabilization;
            const enhancedUndervaluation = property.enhancedUndervaluation;
            
            // Use enhanced analysis for better data quality, fallback to standard
            const finalRentStabilizedConfidence = enhancedRentStabilization?.confidence_percentage || property.rentStabilizedProbability || 0;
            const finalRentStabilizedMethod = enhancedRentStabilization?.analysis_method || this.determineRentStabilizedMethod(property);
            const finalUndervaluationMethod = enhancedUndervaluation?.calculation_methodology?.[0] || this.determineUndervaluationMethod(property);
            
            // RAILWAY FIX: Map enhanced methods to database constraints
            const validRentStabilizedMethod = this.mapEnhancedMethodToDbConstraint(finalRentStabilizedMethod);
            const validUndervaluationMethod = this.mapEnhancedMethodToDbConstraint(finalUndervaluationMethod);
            
            const saveData = {
                listing_id: property.id?.toString(),
                listing_url: property.url || `https://streeteasy.com/rental/${property.id}`,
                address: property.address,
                neighborhood: neighborhood,
                borough: this.getBoroughFromNeighborhood(neighborhood),
                zip_code: property.zipcode,
                
                // Pricing analysis - REQUIRED fields with BULLETPROOF conversion
                monthly_rent: this.safeInt(property.price),
                estimated_market_rent: this.safeInt(property.estimatedMarketRent || property.price),
                undervaluation_percent: this.safeDecimal(property.percentBelowMarket, 1, 0),
                potential_monthly_savings: this.safeInt(property.potentialSavings, 0),
                // potential_annual_savings is GENERATED ALWAYS - don't include
                
                // Property details
                bedrooms: this.safeInt(property.bedrooms, null),
                bathrooms: this.safeInt(property.bathrooms, null),
                sqft: property.sqft > 0 ? this.safeInt(property.sqft) : null,
                description: property.description || null,
                amenities: property.amenities || [],
                building_amenities: property.buildingAmenities || [],
                building_type: property.buildingType || property.propertyType || null,
                year_built: this.safeInt(property.builtIn, null),
                total_units_in_building: this.safeInt(property.totalUnits, null),
                broker_fee: property.brokerFee?.toString() || null,
                available_date: property.availableFrom ? new Date(property.availableFrom).toISOString().split('T')[0] : null,
                
                // Rental terms
                lease_term: property.leaseTerm || null,
                pet_policy: property.petPolicy || null,
                broker_name: property.brokerName || null,
                broker_phone: property.brokerPhone || null,
                broker_email: property.brokerEmail || null,
                listing_agent: property.listingAgent || null,
                
                // Scores
                street_easy_score: this.safeInt(property.streetEasyScore, null),
                walk_score: this.safeInt(property.walkScore, null),
                transit_score: this.safeInt(property.transitScore, null),
                
                // Media
                images: property.images || [],
                virtual_tour_url: property.virtualTourUrl || null,
                floor_plan_url: property.floorPlanUrl || null,
                
                // RENT STABILIZATION - REQUIRED fields with RAILWAY FIX
                rent_stabilized_confidence: this.safeInt(finalRentStabilizedConfidence, 0),
                rent_stabilized_method: validRentStabilizedMethod,
                rent_stabilization_analysis: {
                    explanation: property.rentStabilizedExplanation || "AI-powered analysis based on building characteristics and rent level",
                    key_factors: property.rentStabilizedFactors || [],
                    probability: this.safeInt(finalRentStabilizedConfidence, 0),
                    legal_indicators: this.extractLegalIndicators(property),
                    building_criteria: this.buildingCriteriaAnalysis(property),
                    dhcr_building_match: this.hasDHCRMatch(property),
                    confidence_breakdown: {
                        building_age: property.builtIn && property.builtIn < 1974 ? 30 : 0,
                        rent_level: property.percentBelowMarket > 10 ? 25 : 0,
                        building_type: 20,
                        ai_analysis: 25
                    }
                },
                
                // UNDERVALUATION - REQUIRED fields with RAILWAY FIX
undervaluation_method: validUndervaluationMethod,
undervaluation_confidence: this.safeInt(property.undervaluationConfidence, 0),
comparables_used: Math.max(1, this.safeInt(property.comparablesUsed, 1)),
undervaluation_analysis: property.reasoning || property.detailedAnalysis?.detailed_explanation || 
    `This rent-stabilized property at ${property.address} offers ${property.percentBelowMarket?.toFixed(1)}% savings below estimated market rent of $${property.estimatedMarketRent?.toLocaleString()}/month. The analysis found ${property.comparablesUsed || 0} comparable properties to determine market value.`,

// Scoring and ranking with BULLETPROOF conversion
deal_quality_score: this.safeInt(this.calculateDealQualityScore(property)),
ranking_in_neighborhood: null,
neighborhood_median_rent: null,
comparable_properties_in_area: this.safeInt(property.comparablesUsed, null),
risk_factors: this.identifyRiskFactors(property),
opportunity_score: this.safeInt(this.calculateOpportunityScore(property)),

// Status and metadata
display_status: 'active',
admin_notes: null,
tags: this.generatePropertyTags(property),

// Classification with BULLETPROOF conversion
market_classification: this.classifyRentStabilizedProperty(property),
deal_quality: this.safeDealQuality(this.calculateDealQualityScore(property)),

// Timestamps
discovered_at: new Date().toISOString(),
analyzed_at: new Date().toISOString(),
last_verified: new Date().toISOString(),
analysis_date: new Date().toISOString()
            };
            
            const { error } = await this.supabase
                .from('undervalued_rent_stabilized')
                .upsert(saveData, { onConflict: 'listing_id' });
            
            if (error) {
                console.error(`     ❌ Rent-stabilized save error for ${property.address}: ${error.message}`);
                throw error;
            }
            
        } catch (error) {
            console.error(`     ❌ RAILWAY ERROR saving stabilized property: ${error.message}`);
            throw error;
        }
    }

    /**
     * Determine valid undervaluation_method to match YOUR database constraint
     */
    determineUndervaluationMethod(property) {
        // Valid values from YOUR constraint: 'exact_bed_bath_amenity_match', 'bed_bath_specific_pricing', 'bed_specific_with_adjustments', 'price_per_sqft_fallback'
        
        if (property.analysisMethod) {
            const method = property.analysisMethod.toLowerCase();
            
            if (method.includes('exact') || method.includes('perfect_match')) {
                return 'exact_bed_bath_amenity_match';
            }
            if (method.includes('bed_bath') || method.includes('specific')) {
                return 'bed_bath_specific_pricing';
            }
            if (method.includes('adjustment') || method.includes('comparative')) {
                return 'bed_specific_with_adjustments';
            }
        }
        
        // Default fallback method
        return 'price_per_sqft_fallback';
    }

    /**
     * Extract legal indicators for rent stabilization
     */
    extractLegalIndicators(property) {
        const indicators = [];
        const desc = (property.description || '').toLowerCase();
        
        if (desc.includes('rent stabilized') || desc.includes('rent-stabilized')) {
            indicators.push('explicit_mention');
        }
        if (desc.includes('no broker fee') || property.noFee) {
            indicators.push('no_broker_fee');
        }
        if (desc.includes('long term') || desc.includes('lease renewal')) {
            indicators.push('long_term_lease_options');
        }
        
        return indicators;
    }

    /**
     * Building criteria analysis
     */
    buildingCriteriaAnalysis(property) {
        return {
            age_analysis: property.builtIn ? {
                year_built: property.builtIn,
                is_pre_1974: property.builtIn < 1974,
                stabilization_likelihood: property.builtIn < 1974 ? 'high' : 'medium'
            } : null,
            unit_count_estimate: property.totalUnits || null,
            building_type: property.buildingType || property.propertyType || 'apartment',
            rent_level_analysis: {
                monthly_rent: property.price,
                below_market_indicator: property.percentBelowMarket > 0
            }
        };
    }

    /**
     * Check for DHCR match
     */
    hasDHCRMatch(property) {
        return property.rentStabilizedFactors ? 
            property.rentStabilizedFactors.some(factor => 
                factor.includes('dhcr') || factor.includes('database')
            ) : false;
    }

    /**
     * Identify risk factors
     */
    identifyRiskFactors(property) {
        const risks = [];
        
        if (!property.sqft || property.sqft === 0) {
            risks.push('missing_square_footage');
        }
        if (property.percentBelowMarket < 5) {
            risks.push('minimal_undervaluation');
        }
        if (property.undervaluationConfidence < 70) {
            risks.push('low_analysis_confidence');
        }
        
        return risks;
    }

    /**
     * Calculate opportunity score
     */
    calculateOpportunityScore(property) {
        let score = 50; // Base score
        
        // Undervaluation impact
        score += Math.min(25, property.percentBelowMarket || 0);
        
        // Stabilization impact  
        score += Math.round((property.rentStabilizedProbability || 0) * 0.25);
        
        // Confidence bonus
        score += Math.round((property.undervaluationConfidence || 0) * 0.1);
        
        return Math.max(0, Math.min(100, score));
    }

    /**
     * FIXED: Save to undervalued_rentals table using Supabase (NO net.http_post)
     */
    async saveToUndervaluedRentalsTable(property, neighborhood) {
        try {
            const saveData = {
                listing_id: property.id?.toString(),
                address: property.address,
                neighborhood: neighborhood,
                borough: this.getBoroughFromNeighborhood(neighborhood),
                zipcode: property.zipcode,
                
                // Pricing analysis with BULLETPROOF conversion
                monthly_rent: this.safeInt(property.price),
                discount_percent: this.safeDecimal(property.percentBelowMarket, 1, 0),
                potential_monthly_savings: this.safeInt(property.potentialSavings, 0),
                annual_savings: this.safeInt((property.potentialSavings || 0) * 12),
                
                // Property details with BULLETPROOF conversion
                bedrooms: this.safeInt(property.bedrooms, 0),
                bathrooms: this.safeInt(property.bathrooms, 0),
                sqft: this.safeInt(property.sqft, 0),
                property_type: property.propertyType || 'apartment',
                
                // Rental terms
                no_fee: property.noFee || false,
                available_from: property.availableFrom ? new Date(property.availableFrom).toISOString() : null,
                
                // Building features
                doorman_building: this.hasAmenity(property.amenities, ['doorman']),
                elevator_building: this.hasAmenity(property.amenities, ['elevator']),
                pet_friendly: this.hasAmenity(property.amenities, ['pet', 'dog', 'cat']),
                laundry_available: this.hasAmenity(property.amenities, ['laundry', 'washer']),
                gym_available: this.hasAmenity(property.amenities, ['gym', 'fitness']),
                rooftop_access: this.hasAmenity(property.amenities, ['rooftop', 'roof']),
                
                // Building info with BULLETPROOF conversion
                built_in: this.safeInt(property.builtIn, null),
                
                // Media and description
                images: property.images || [],
                image_count: this.safeInt((property.images || []).length),
                description: property.description || '',
                amenities: property.amenities || [],
                amenity_count: this.safeInt((property.amenities || []).length),
                
                // Analysis results with BULLETPROOF conversion
                score: this.safeInt(this.calculatePropertyScore(property)),
                grade: this.calculatePropertyGrade(this.calculatePropertyScore(property)),
                deal_quality: this.safeDealQuality(this.calculatePropertyScore(property)),
                reasoning: property.reasoning || 'AI-powered market analysis',
                comparison_group: neighborhood,
                comparison_method: property.analysisMethod || 'claude_ai',
                reliability_score: this.safeInt(property.undervaluationConfidence, 0),
                
                // Metadata
                analysis_date: new Date().toISOString(),
                status: 'active',
                last_seen_in_search: new Date().toISOString(),
                times_seen_in_search: 1
            };
            
            const { error } = await this.supabase
                .from('undervalued_rentals')
                .upsert(saveData, { onConflict: 'listing_id' });
            
            if (error) {
                console.error(`     ❌ Undervalued rentals save error for ${property.address}: ${error.message}`);
                throw error;
            }
            
        } catch (error) {
            console.error(`     ❌ RAILWAY ERROR saving undervalued property: ${error.message}`);
            throw error;
        }
    }

    /**
     * FIXED: Determine valid rent_stabilized_method to match YOUR database constraint
     */
    determineRentStabilizedMethod(property) {
        // Valid values from YOUR constraint: 'explicit_mention', 'dhcr_registered', 'circumstantial', 'building_analysis'
        
        if (property.rentStabilizedFactors && property.rentStabilizedFactors.length > 0) {
            const factors = property.rentStabilizedFactors;
            
            // Check for explicit mentions in description
            if (factors.includes('explicit_stabilization_mention') || 
                factors.includes('rent_stabilized_listed')) {
                return 'explicit_mention';
            }
            
            // Check for DHCR database matches
            if (factors.includes('dhcr_match') || 
                factors.includes('database_match') || 
                factors.includes('dhcr_building_match')) {
                return 'dhcr_registered';
            }
            
            // Check for circumstantial evidence (rent level, no fee, etc.)
            if (factors.includes('below_market_suggests_stabilization') || 
                factors.includes('no_fee_indicator') ||
                factors.includes('long_term_tenancy_suggestion')) {
                return 'circumstantial';
            }
        }
        
        // Default to building analysis (age, unit count, etc.)
        return 'building_analysis';
    }

    /**
     * Check if property has specific amenities
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
     * BULLETPROOF: Convert any value to integer safely
     */
    safeInt(value, defaultValue = 0) {
        if (value === null || value === undefined) return defaultValue;
        const num = parseFloat(value);
        return isNaN(num) ? defaultValue : Math.round(num);
    }

    /**
     * BULLETPROOF: Convert any value to decimal safely
     */
    safeDecimal(value, decimals = 2, defaultValue = 0) {
        if (value === null || value === undefined) return defaultValue;
        const num = parseFloat(value);
        return isNaN(num) ? defaultValue : parseFloat(num.toFixed(decimals));
    }

    /**
     * BULLETPROOF: Ensure deal quality is valid constraint value
     */
    safeDealQuality(score) {
        const intScore = this.safeInt(score, 50);
        if (intScore >= 98) return 'exceptional';
        if (intScore >= 90) return 'excellent'; 
        if (intScore >= 80) return 'very_good';
        if (intScore >= 70) return 'good';
        if (intScore >= 60) return 'fair';
        return 'poor';
    }

    /**
     * Calculate property score (0-100)
     */
    calculatePropertyScore(property) {
        let score = 50; // Base score
        
        // Undervaluation bonus
        if (property.percentBelowMarket > 0) {
            score += Math.min(30, property.percentBelowMarket);
        }
        
        // Rent stabilization bonus
        if (property.rentStabilizedProbability > 0) {
            score += Math.round(property.rentStabilizedProbability * 0.2);
        }
        
        // Confidence bonus
        score += Math.round((property.undervaluationConfidence || 0) * 0.1);
        
        // Amenity bonus
        score += Math.min(10, (property.amenities?.length || 0));
        
        return Math.max(0, Math.min(100, score));
    }

    /**
     * Calculate property grade based on score
     */
    calculatePropertyGrade(score) {
        if (score >= 98) return 'A+';
        if (score >= 93) return 'A';
        if (score >= 88) return 'B+';
        if (score >= 83) return 'B';
        if (score >= 75) return 'C+';
        if (score >= 65) return 'C';
        if (score >= 60) return 'D';
        return 'F';
    }

    /**
     * Calculate deal quality score for rent-stabilized properties
     */
    calculateDealQualityScore(property) {
        let score = 50; // Base score
        
        // Market position impact
        score += Math.round(property.percentBelowMarket * 0.8);
        
        // Stabilization probability impact
        score += Math.round(property.rentStabilizedProbability * 0.3);
        
        // Undervaluation confidence bonus
        score += Math.round((property.undervaluationConfidence || 0) * 0.05);
        
        return Math.max(0, Math.min(100, score));
    }

    /**
     * Calculate deal quality from score
     */
    calculateDealQuality(score) {
        if (score >= 98) return 'exceptional';
        if (score >= 90) return 'excellent';
        if (score >= 80) return 'very_good';
        if (score >= 70) return 'good';
        if (score >= 60) return 'fair';
        return 'poor';
    }

    /**
     * Classify rent-stabilized property
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
        if ((property.percentBelowMarket || 0) >= 25) tags.push('exceptional_deal');
        
        return tags;
    }

    /**
 * RESTORED: Get borough from neighborhood (REQUIRED for data integrity) - UPDATED WITH NEW NEIGHBORHOODS
 */
getBoroughFromNeighborhood(neighborhood) {
    const manhattanNeighborhoods = [
        'soho', 'tribeca', 'west-village', 'east-village', 'lower-east-side',
        'financial-district', 'battery-park-city', 'chinatown', 'little-italy',
        'nolita', 'midtown', 'two-bridges', 'chelsea', 'gramercy-park', 'kips-bay',
        'murray-hill', 'midtown-east', 'midtown-west', 'hells-kitchen',
        'upper-east-side', 'upper-west-side', 'morningside-heights',
        'hamilton-heights', 'washington-heights', 'inwood', 'greenwich-village',
        'flatiron', 'noho', 'civic-center', 'battery-park-city', 'hudson-square',
        // NEW MANHATTAN NEIGHBORHOODS ADDED:
        'roosevelt-island', 'hudson-yards', 'central-harlem', 'nomad', 'manhattan-valley'
    ];
    
    const brooklynNeighborhoods = [
        'brooklyn-heights', 'dumbo', 'williamsburg', 'greenpoint', 'park-slope',
        'carroll-gardens', 'cobble-hill', 'boerum-hill', 'fort-greene',
        'prospect-heights', 'crown-heights', 'bedford-stuyvesant', 'bushwick', 'gowanus', 'clinton-hill', 'red-hook',
        'prospect-lefferts-gardens', 'sunset-park', 'bay-ridge', 'bensonhurst', 'downtown-brooklyn', 
        'columbia-st-waterfront-district', 'vinegar-hill',
        // NEW BROOKLYN NEIGHBORHOODS ADDED:
        'windsor-terrace', 'ditmas-park'
    ];
    
    const queensNeighborhoods = [
        'long-island-city', 'astoria', 'sunnyside', 'woodside', 'elmhurst',
        'jackson-heights', 'corona', 'flushing', 'forest-hills', 'ridgewood',
        'maspeth', 'rego-park',
        // NEW QUEENS NEIGHBORHOODS ADDED:
        'bayside', 'fresh-meadows', 'ditmars-steinway', 'briarwood'
    ];
    
    const bronxNeighborhoods = [
        'mott-haven', 'melrose', 'concourse', 'yankee-stadium', 'fordham',
        'belmont', 'tremont', 'mount-eden', 'south-bronx', 'highbridge',
        'university-heights', 'morrisania',
        // NEW BRONX NEIGHBORHOODS ADDED:
        'kingsbridge', 'norwood', 'pelham-bay'
    ];
    
    const normalizedNeighborhood = neighborhood.toLowerCase();
    
    if (manhattanNeighborhoods.includes(normalizedNeighborhood)) return 'Manhattan';
    if (brooklynNeighborhoods.includes(normalizedNeighborhood)) return 'Brooklyn';
    if (queensNeighborhoods.includes(normalizedNeighborhood)) return 'Queens';
    if (bronxNeighborhoods.includes(normalizedNeighborhood)) return 'Bronx';
    
    return 'Unknown';
}
    /**
     * Helper function for delays
     */
    delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    /**
     * RESTORED: Load rent-stabilized buildings from Supabase (CRITICAL for accuracy)
     */
    async loadRentStabilizedBuildings() {
        try {
            let allBuildings = [];
            let offset = 0;
            const batchSize = 1000;
            let hasMoreData = true;
            
            console.log('🏢 Loading rent-stabilized buildings from DHCR data...');
            
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
                
                // Robust stopping condition
                if (!data || data.length === 0) {
                    hasMoreData = false;
                    break;
                }
                
                allBuildings = allBuildings.concat(data);
                console.log(`   ✅ Loaded ${data.length} buildings (total: ${allBuildings.length})`);
                
                // Dynamic continuation
                if (data.length < batchSize) {
                    hasMoreData = false;
                }
                
                offset += batchSize;
                
                // Safety check
                if (offset > 100000) {
                    console.log('   ⚠️ Reached safety limit of 100,000 buildings');
                    hasMoreData = false;
                }
            }
            
            console.log(`   ✅ Loaded ${allBuildings.length} total rent-stabilized buildings`);
            return allBuildings;
            
        } catch (error) {
            console.error('Failed to load rent-stabilized buildings:', error.message);
            console.log('   🔄 Continuing without DHCR data - Claude will use other indicators');
            return [];
        }
    }

    /**
     * RESTORED: Main comprehensive analysis entry point (NEEDED for Railway)
     */
    async runComprehensiveRentalAnalysis(options = {}) {
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
            const rentStabilizedBuildings = await this.loadRentStabilizedBuildings();
            
            // Step 2: Determine neighborhoods to process
            const neighborhoods = options.neighborhoods || 
                                (process.env.TEST_NEIGHBORHOOD ? [process.env.TEST_NEIGHBORHOOD] : 
                                 this.getHighPriorityNeighborhoods());
            
            console.log(`🎯 Processing ${neighborhoods.length} neighborhoods...`);
            
            // Step 3: Process each neighborhood
            let totalCacheHits = 0;
            let totalApiCalls = 0;
            
            for (const neighborhood of neighborhoods) {
                try {
                    console.log(`\n📍 Analyzing ${neighborhood}...`);
                    
                    const neighborhoodResults = await this.analyzeNeighborhoodForRentStabilized(neighborhood, {
                        rentStabilizedBuildings
                    });
                    
                    // Update totals
                    results.undervaluedRentals += neighborhoodResults.undervaluedCount;
                    results.rentStabilizedFound += neighborhoodResults.stabilizedCount;
                    results.undervaluedStabilized += neighborhoodResults.undervaluedStabilizedCount;
                    results.totalAnalyzed += neighborhoodResults.totalAnalyzed;
                    results.neighborhoodsProcessed++;
                    
                    console.log(`   ✅ ${neighborhood}: ${neighborhoodResults.undervaluedCount} undervalued, ${neighborhoodResults.stabilizedCount} stabilized`);
                    
                } catch (error) {
                    console.error(`   ❌ Error in ${neighborhood}: ${error.message}`);
                    results.errors.push({ neighborhood, error: error.message });
                }
                
                // Rate limiting between neighborhoods
                await this.delay(1000);
            }
            
            // Final results
            const duration = (Date.now() - startTime) / 1000;
            results.apiCallsUsed = this.claudeAnalyzer.apiCallsUsed;
            
            console.log('\n🎉 CLAUDE ANALYSIS COMPLETE!');
            console.log('=' .repeat(60));
            console.log(`⏱️ Duration: ${Math.round(duration)}s`);
            console.log(`📊 Total analyzed: ${results.totalAnalyzed} properties`);
            console.log(`🏠 Undervalued rentals: ${results.undervaluedRentals}`);
            console.log(`🔒 Rent-stabilized found: ${results.rentStabilizedFound}`);
            console.log(`💎 Undervalued + Stabilized: ${results.undervaluedStabilized}`);
            console.log(`🤖 Claude API calls: ${results.apiCallsUsed}`);
            console.log(`💰 Estimated cost: ${(results.apiCallsUsed * 0.0006).toFixed(3)}`);
            
            return results;
            
        } catch (error) {
            console.error('💥 ANALYSIS FAILED:', error.message);
            throw error;
        }
    }

    /**
     * RESTORED: Get borough from neighborhood (REQUIRED for data integrity)
     */
    getBoroughFromNeighborhood(neighborhood) {
        const manhattanNeighborhoods = [
            'soho', 'tribeca', 'west-village', 'east-village', 'lower-east-side',
            'financial-district', 'battery-park-city', 'chinatown', 'little-italy',
            'nolita', 'midtown', 'two-bridges', 'chelsea', 'gramercy-park', 'kips-bay',
            'murray-hill', 'midtown-east', 'midtown-west', 'hells-kitchen',
            'upper-east-side', 'upper-west-side', 'morningside-heights',
            'hamilton-heights', 'washington-heights', 'inwood', 'greenwich-village',
            'flatiron', 'noho', 'civic-center', 'battery-park-city', 'hudson-square'
        ];
        
        const brooklynNeighborhoods = [
            'brooklyn-heights', 'dumbo', 'williamsburg', 'greenpoint', 'park-slope',
            'carroll-gardens', 'cobble-hill', 'boerum-hill', 'fort-greene',
            'prospect-heights', 'crown-heights', 'bedford-stuyvesant', 'bushwick', 'gowanus', 'clinton-hill', 'red-hook',
            'prospect-lefferts-gardens', 'sunset-park', 'bay-ridge', 'bensonhurst', 'downtown-brooklyn', 'columbia-st-waterfront-district', 'vinegar-hill'
        ];
        
        const queensNeighborhoods = [
            'long-island-city', 'astoria', 'sunnyside', 'woodside', 'elmhurst',
            'jackson-heights', 'corona', 'flushing', 'forest-hills', 'ridgewood',
            'maspeth', 'rego-park'
        ];
        
        const bronxNeighborhoods = [
            'mott-haven', 'melrose', 'concourse', 'yankee-stadium', 'fordham',
            'belmont', 'tremont', 'mount-eden', 'south-bronx', 'highbridge',
            'university-heights', 'morrisania'
        ];
        
        const normalizedNeighborhood = neighborhood.toLowerCase();
        
        if (manhattanNeighborhoods.includes(normalizedNeighborhood)) return 'Manhattan';
        if (brooklynNeighborhoods.includes(normalizedNeighborhood)) return 'Brooklyn';
        if (queensNeighborhoods.includes(normalizedNeighborhood)) return 'Queens';
        if (bronxNeighborhoods.includes(normalizedNeighborhood)) return 'Bronx';
        
        return 'Unknown';
    }

   /**
 * RESTORED: Get high-priority neighborhoods for analysis - UPDATED WITH NEW NEIGHBORHOODS
 */
getHighPriorityNeighborhoods() {
    // NEW: If testing single listing, just use one neighborhood
    if (this.testSingleListing) {
        return ['dumbo']; // Use dumbo since it worked well before
    }
    
    return [
          // Manhattan priority areas (existing + new)
        'soho', 'tribeca', 'west-village', 'east-village', 'lower-east-side',
        'financial-district', 'battery-park-city', 'chinatown', 'little-italy',
        'nolita', 'midtown', 'two-bridges', 'chelsea', 'gramercy-park', 'kips-bay',
        'murray-hill', 'midtown-east', 'midtown-west', 'hells-kitchen',
        'upper-east-side', 'upper-west-side', 'morningside-heights',
        'hamilton-heights', 'washington-heights', 'inwood', 'greenwich-village',
        'flatiron', 'noho', 'civic-center', 'hudson-square', 'roosevelt-island', 'hudson-yards', 'nomad', 'manhattan-valley', 'central-harlem',
        
        // Brooklyn priority areas (existing + new)
         'brooklyn-heights', 'dumbo', 'williamsburg', 'greenpoint', 'park-slope',
        'carroll-gardens', 'cobble-hill', 'boerum-hill', 'fort-greene',
        'prospect-heights', 'crown-heights', 'bedford-stuyvesant', 'bushwick', 
        'gowanus', 'clinton-hill', 'red-hook', 'prospect-lefferts-gardens', 
        'sunset-park', 'bay-ridge', 'downtown-brooklyn', 
        'columbia-st-waterfront-district', 'vinegar-hill',
        // NEW BROOKLYN NEIGHBORHOODS ADDED:
        'windsor-terrace', 'ditmas-park',
        
        // Queens priority areas (existing + new)
       'long-island-city', 'astoria', 'sunnyside', 'woodside', 'elmhurst',
        'jackson-heights', 'corona', 'flushing', 'forest-hills', 'ridgewood',
        'maspeth', 'rego-park',
        // NEW QUEENS NEIGHBORHOODS ADDED:
        'bayside', 'ditmars-steinway',
        
        // Bronx priority areas (existing + new)
        'mott-haven', 'melrose', 'concourse', 'south-bronx',
        'kingsbridge', 'norwood'
    ];
}
    /**
     * RESTORED: Get analysis summary (Useful for debugging)
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
                goldmineDeals: goldmine || [] // Both undervalued AND stabilized
            };

        } catch (error) {
            console.error('Error getting summary:', error.message);
            return { topUndervalued: [], topStabilized: [], goldmineDeals: [] };
        }
    }

    /**
     * RESTORED: Get usage statistics (Cost tracking)
     */
    getUsageStats() {
        return {
            apiCallsUsed: this.claudeAnalyzer.apiCallsUsed,
            estimatedCost: this.claudeAnalyzer.apiCallsUsed * 0.0006,
            cacheEfficiency: 0 // Can be enhanced later
        };
    }

    /**
     * Run comprehensive rent-stabilized analysis for multiple neighborhoods
     */
    async runComprehensiveAnalysis(neighborhoods, options = {}) {
        console.log('\n🏙️ Starting comprehensive rent-stabilized analysis...');
        console.log(`   🎯 Target neighborhoods: ${neighborhoods.join(', ')}`);
        
        const overallResults = {
            totalNeighborhoods: neighborhoods.length,
            totalListings: 0,
            totalAnalyzed: 0,
            undervaluedCount: 0,
            stabilizedCount: 0,
            undervaluedStabilizedCount: 0,
            savedCount: 0,
            neighborhoodResults: [],
            startTime: new Date(),
            errors: []
        };
        
        for (const neighborhood of neighborhoods) {
            try {
                const result = await this.analyzeNeighborhoodForRentStabilized(neighborhood, options);
                
                overallResults.totalListings += result.totalListings;
                overallResults.totalAnalyzed += result.totalAnalyzed;
                overallResults.undervaluedCount += result.undervaluedCount;
                overallResults.stabilizedCount += result.stabilizedCount;
                overallResults.undervaluedStabilizedCount += result.undervaluedStabilizedCount;
                overallResults.savedCount += result.savedCount;
                overallResults.neighborhoodResults.push(result);
                overallResults.errors.push(...result.errors);
                
            } catch (error) {
                console.error(`❌ Failed to analyze ${neighborhood}: ${error.message}`);
                overallResults.errors.push(`${neighborhood}: ${error.message}`);
            }
            
            // Delay between neighborhoods
            await this.delay(1000);
        }
        
        overallResults.endTime = new Date();
        overallResults.duration = Math.round((overallResults.endTime - overallResults.startTime) / 1000);
        
        this.printFinalResults(overallResults);
        return overallResults;
    }

    /**
     * Print final analysis results
     */
    printFinalResults(results) {
        console.log('\n' + '='.repeat(60));
        console.log(`⏱️ Duration: ${results.duration}s`);
        console.log(`🏘️ Neighborhoods: ${results.totalNeighborhoods}`);
        console.log(`📊 Total analyzed: ${results.totalAnalyzed} properties`);
        console.log(`🏠 Undervalued rentals: ${results.undervaluedCount}`);
        console.log(`🔒 Rent-stabilized found: ${results.stabilizedCount}`);
        console.log(`💎 Undervalued + Stabilized: ${results.undervaluedStabilizedCount}`);
        console.log(`💾 Total saved: ${results.savedCount}`);
        console.log(`🤖 Claude API calls: ${this.claudeAnalyzer.apiCallsUsed}`);
        
        if (results.errors.length > 0) {
            console.log(`⚠️ Errors: ${results.errors.length}`);
        }
        
        console.log('='.repeat(60));
        console.log('\n🎉 CLAUDE RENTALS ANALYSIS COMPLETE!');
    }
}

module.exports = ClaudePoweredRentalsSystem;
