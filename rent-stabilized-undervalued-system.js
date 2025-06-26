// rent-stabilized-undervalued-system.js
// PRODUCTION-GRADE: Find rent-stabilized listings + determine which are undervalued
// COMPLETE FILE: Full advanced bed/bath/amenities analysis from biweekly-rentals system

require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

// DHCR File parsing dependencies (install these)
const Papa = require('papaparse');     // npm install papaparse
const pdf = require('pdf-parse');      // npm install pdf-parse  
const XLSX = require('xlsx');          // npm install xlsx

class RentStabilizedUndervaluedDetector {
    constructor() {
        this.supabase = createClient(
            process.env.SUPABASE_URL,
            process.env.SUPABASE_ANON_KEY
        );

        // STEP 1: Rent-stabilized detection (legal indicators ONLY)
        this.RENT_STABILIZED_INDICATORS = {
            explicit: {
                'rent stabilized': 100,
                'rent-stabilized': 100,
                'stabilized unit': 95,
                'stabilized apartment': 95,
                'dhcr registered': 90,
                'legal rent': 85,
                'preferential rent': 90,
                'regulated apartment': 85,
                'rgb increase': 80,
                'lease renewal': 75,
                'renewal lease': 75
            },
            legal_building: {
                prewar_6plus: 85,        // Pre-1947 + 6+ units
                golden_age: 80,          // 1947-1973 + 6+ units  
                post74_tax_benefit: 75   // Post-1974 + tax benefits
            },
            circumstantial: {
                'prewar': 45,
                'pre-war': 45,
                'walk-up': 40,
                'walkup': 40,
                'tenant in place': 35,
                'established tenant': 30,
                'original details': 25
            }
        };

        // STEP 2: Undervaluation analysis (sophisticated market analysis)
        this.UNDERVALUATION_THRESHOLD = 15; // 15%+ below market = undervalued
        this.VALUATION_METHODS = {
            EXACT_MATCH: 'exact_bed_bath_amenity_match',
            BED_BATH_SPECIFIC: 'bed_bath_specific_pricing',
            BED_SPECIFIC: 'bed_specific_with_adjustments',
            PRICE_PER_SQFT_FALLBACK: 'price_per_sqft_fallback'
        };

        // Minimum sample sizes for each method (from biweekly-rentals)
        this.MIN_SAMPLES = {
            EXACT_MATCH: 3,
            BED_BATH_SPECIFIC: 8,
            BED_SPECIFIC: 12,
            PRICE_PER_SQFT_FALLBACK: 20
        };

        // Listing cache to avoid duplicate fetches
        this.listingCache = new Map();
        this.cacheExpiry = 7 * 24 * 60 * 60 * 1000; // 7 days
    }

    /**
     * MAIN FUNCTION: Find rent-stabilized listings that are ALSO undervalued
     */
    async findUndervaluedRentStabilizedListings(options = {}) {
        console.log('🏠 Finding rent-stabilized listings that are UNDERVALUED...\n');

        const {
            neighborhoods = ['east-village', 'lower-east-side', 'chinatown'],
            maxListingsPerNeighborhood = 100,
            testMode = false
        } = options;

        try {
            // Step 1: Get ALL listings in target neighborhoods (with caching)
            console.log('📋 Step 1: Fetching all listings (with cache)...');
            const allListings = await this.getAllListingsWithCache(neighborhoods, maxListingsPerNeighborhood);
            console.log(`   ✅ Total listings: ${allListings.length}\n`);

            // Step 2: Load rent-stabilized buildings database
            console.log('🏢 Step 2: Loading rent-stabilized buildings...');
            const stabilizedBuildings = await this.loadRentStabilizedBuildings();
            console.log(`   ✅ Stabilized buildings: ${stabilizedBuildings.length}\n`);

            // Step 3: Find rent-stabilized listings using LEGAL INDICATORS ONLY
            console.log('⚖️ Step 3: Identifying rent-stabilized listings...');
            const rentStabilizedListings = await this.identifyRentStabilizedListings(
                allListings, 
                stabilizedBuildings
            );
            console.log(`   ✅ Rent-stabilized found: ${rentStabilizedListings.length}\n`);

            // Step 4: Determine which rent-stabilized listings are UNDERVALUED
            console.log('💰 Step 4: Analyzing undervaluation of rent-stabilized listings...');
            const undervaluedStabilized = await this.analyzeUndervaluationOfStabilized(
                rentStabilizedListings,
                allListings
            );
            console.log(`   ✅ Undervalued rent-stabilized: ${undervaluedStabilized.length}\n`);

            // Step 5: Save results and generate report
            await this.saveResults(undervaluedStabilized);
            this.generateFinalReport(undervaluedStabilized);

            return {
                totalListingsScanned: allListings.length,
                rentStabilizedFound: rentStabilizedListings.length,
                undervaluedStabilizedFound: undervaluedStabilized.length,
                results: undervaluedStabilized.sort((a, b) => b.undervaluationPercent - a.undervaluationPercent)
            };

        } catch (error) {
            console.error('💥 Analysis failed:', error.message);
            throw error;
        }
    }

    /**
     * STEP 1: Get all listings with intelligent caching
     */
    async getAllListingsWithCache(neighborhoods, maxPerNeighborhood) {
        const allListings = [];
        
        for (const neighborhood of neighborhoods) {
            console.log(`   📍 Fetching ${neighborhood}...`);
            
            try {
                // Check cache first
                const cachedListings = await this.getCachedListings(neighborhood);
                const freshListings = await this.fetchNewListings(neighborhood, cachedListings, maxPerNeighborhood);
                
                // Combine cached + fresh
                const neighborhoodListings = [...cachedListings, ...freshListings];
                allListings.push(...neighborhoodListings);
                
                console.log(`     ✅ ${cachedListings.length} cached + ${freshListings.length} fresh = ${neighborhoodListings.length} total`);
                
                // Update cache
                await this.updateListingCache(freshListings);
                
            } catch (error) {
                console.error(`     ❌ Failed to fetch ${neighborhood}:`, error.message);
                continue;
            }
        }
        
        return allListings;
    }

    /**
     * Get cached listings (no time expiry - we use smart ID-based cleanup)
     */
    async getCachedListings(neighborhood) {
        try {
            const { data, error } = await this.supabase
                .from('listing_cache')
                .select('*')
                .eq('neighborhood', neighborhood);
                // Removed: .gte('cached_at', new Date(Date.now() - this.cacheExpiry).toISOString())
            
            if (error) throw error;
            
            return (data || []).map(row => ({
                id: row.listing_id,
                address: row.address,
                price: row.monthly_rent,
                bedrooms: row.bedrooms,
                bathrooms: row.bathrooms,
                sqft: row.sqft,
                description: row.description,
                neighborhood: row.neighborhood,
                amenities: row.amenities || [],
                url: row.listing_url,
                listedAt: row.listed_at,
                source: 'cache'
            }));
            
        } catch (error) {
            console.error('Cache lookup failed:', error.message);
            return [];
        }
    }

    /**
     * Fetch new listings not in cache using smart ID-based caching strategy
     */
    async fetchNewListings(neighborhood, cachedListings, maxListings) {
        const freshListings = [];
        
        try {
            console.log(`       🔍 Smart caching: Checking ${neighborhood} for new listings...`);
            
            // STEP 1: Get neighborhood listing IDs using existing cache infrastructure
            const neighborhoodListingIds = await this.getNeighborhoodListingIdsFromCache(neighborhood, maxListings);
            console.log(`       📋 Found ${neighborhoodListingIds.length} listing IDs in ${neighborhood}`);
            
            if (neighborhoodListingIds.length === 0) {
                console.log(`       ⚠️ No listings found in ${neighborhood}`);
                return [];
            }
            
            // STEP 2: Filter out already cached listing IDs (avoid duplicate fetches)
            const cachedIds = new Set(cachedListings.map(listing => listing.id));
            const newListingIds = neighborhoodListingIds.filter(id => !cachedIds.has(id));
            
            console.log(`       💾 Cache hit: ${cachedListings.length} listings already cached`);
            console.log(`       🆕 New listings to fetch: ${newListingIds.length}`);
            
            // STEP 3: Remove stale listings that disappeared from StreetEasy
            const currentIds = new Set(neighborhoodListingIds);
            const staleListings = cachedListings.filter(cached => !currentIds.has(cached.id));
            
            if (staleListings.length > 0) {
                console.log(`       🗑️ Removing ${staleListings.length} stale listings no longer on StreetEasy...`);
                await this.removeStaleListings(staleListings, neighborhood);
            }
            
            if (newListingIds.length === 0) {
                console.log(`       ✅ All current listings already cached - 100% API savings!`);
                return [];
            }
            
            // STEP 4: Fetch detailed data from existing cache infrastructure
            console.log(`       📡 Fetching detailed data for ${newListingIds.length} new listings...`);
            
            const detailedListings = await this.fetchListingDetailsFromCache(newListingIds, neighborhood);
            freshListings.push(...detailedListings);
            
            console.log(`       🎉 Successfully fetched ${detailedListings.length}/${newListingIds.length} new listings`);
            
            // Calculate API efficiency
            const totalPossibleCalls = neighborhoodListingIds.length;
            const actualCalls = newListingIds.length;
            const efficiency = totalPossibleCalls > 0 ? 
                ((totalPossibleCalls - actualCalls) / totalPossibleCalls * 100).toFixed(1) : 0;
            
            console.log(`       ⚡ API efficiency: ${efficiency}% (saved ${totalPossibleCalls - actualCalls} calls)`);
            
            if (staleListings.length > 0) {
                console.log(`       🧹 Cache cleanup: Removed ${staleListings.length} stale listings`);
            }
            
            return freshListings;
            
        } catch (error) {
            console.error(`Smart fetching failed for ${neighborhood}:`, error.message);
            return freshListings; // Return what we got so far
        }
    }

    /**
     * FIXED: Get neighborhood listing IDs from existing cache infrastructure
     */
    async getNeighborhoodListingIdsFromCache(neighborhood, maxListings) {
        try {
            console.log(`         🗃️ Checking existing cache for ${neighborhood} listing IDs...`);
            
            // Check rental_market_cache first (from biweekly-rentals system)
            const { data: rentalIds, error: rentalError } = await this.supabase
                .from('rental_market_cache')
                .select('listing_id')
                .eq('neighborhood', neighborhood)
                .eq('status', 'active')
                .limit(maxListings)
                .order('last_seen_in_search', { ascending: false });
            
            if (rentalError) {
                console.error(`         ❌ Error fetching from rental_market_cache:`, rentalError.message);
            }
            
            const rentalListingIds = (rentalIds || []).map(row => row.listing_id);
            
            if (rentalListingIds.length > 0) {
                console.log(`         ✅ Found ${rentalListingIds.length} rental IDs from existing cache`);
                return rentalListingIds;
            }
            
            // Fallback: Check listing_cache
            const { data: cacheIds, error: cacheError } = await this.supabase
                .from('listing_cache')
                .select('listing_id')
                .eq('neighborhood', neighborhood)
                .limit(maxListings);
            
            if (cacheError) {
                console.error(`         ❌ Error fetching from listing_cache:`, cacheError.message);
                return [];
            }
            
            const cacheListingIds = (cacheIds || []).map(row => row.listing_id);
            console.log(`         ✅ Found ${cacheListingIds.length} IDs from listing_cache`);
            
            return cacheListingIds;
            
        } catch (error) {
            console.error(`Failed to get ${neighborhood} listing IDs:`, error.message);
            return [];
        }
    }

    /**
     * FIXED: Fetch listing details from existing cache infrastructure
     */
    async fetchListingDetailsFromCache(listingIds, neighborhood) {
        if (listingIds.length === 0) return [];
        
        try {
            console.log(`           📚 Fetching details for ${listingIds.length} listings from cache...`);
            
            // Try rental_market_cache first (most complete data)
            const { data: rentalData, error: rentalError } = await this.supabase
                .from('rental_market_cache')
                .select('*')
                .in('listing_id', listingIds);
            
            if (rentalError) {
                console.error(`           ❌ Error fetching rental details:`, rentalError.message);
            }
            
            const detailedListings = [];
            const foundRentalIds = new Set();
            
            // Process rental cache data
            if (rentalData && rentalData.length > 0) {
                for (const row of rentalData) {
                    foundRentalIds.add(row.listing_id);
                    detailedListings.push({
                        id: row.listing_id,
                        address: row.address,
                        price: row.monthly_rent,
                        bedrooms: row.bedrooms,
                        bathrooms: row.bathrooms,
                        sqft: row.sqft,
                        description: row.description,
                        neighborhood: row.neighborhood,
                        amenities: row.amenities || [],
                        url: row.listing_url,
                        listedAt: row.listed_at,
                        source: 'rental_cache'
                    });
                }
                console.log(`           ✅ Found ${detailedListings.length} complete listings in rental_market_cache`);
            }
            
            // Check listing_cache for remaining IDs
            const remainingIds = listingIds.filter(id => !foundRentalIds.has(id));
            if (remainingIds.length > 0) {
                const { data: cacheData, error: cacheError } = await this.supabase
                    .from('listing_cache')
                    .select('*')
                    .in('listing_id', remainingIds);
                
                if (cacheError) {
                    console.error(`           ❌ Error fetching from listing_cache:`, cacheError.message);
                } else if (cacheData && cacheData.length > 0) {
                    for (const row of cacheData) {
                        detailedListings.push({
                            id: row.listing_id,
                            address: row.address,
                            price: row.monthly_rent,
                            bedrooms: row.bedrooms,
                            bathrooms: row.bathrooms,
                            sqft: row.sqft,
                            description: row.description,
                            neighborhood: row.neighborhood,
                            amenities: row.amenities || [],
                            url: row.listing_url,
                            listedAt: row.listed_at,
                            source: 'listing_cache'
                        });
                    }
                    console.log(`           ✅ Found ${cacheData.length} additional listings in listing_cache`);
                }
            }
            
            return detailedListings;
            
        } catch (error) {
            console.error('Failed to fetch listing details from cache:', error.message);
            return [];
        }
    }

    /**
     * Remove stale listings that no longer exist on StreetEasy
     */
    async removeStaleListings(staleListings, neighborhood) {
        if (staleListings.length === 0) return;
        
        try {
            // Extract listing IDs to remove
            const staleIds = staleListings.map(listing => listing.id);
            
            console.log(`         🗑️ Removing stale listings: ${staleIds.join(', ')}`);
            
            // Remove from listing_cache table
            const { error: cacheError } = await this.supabase
                .from('listing_cache')
                .delete()
                .in('listing_id', staleIds)
                .eq('neighborhood', neighborhood);
            
            if (cacheError) {
                console.error(`Failed to remove stale listings from cache:`, cacheError.message);
            } else {
                console.log(`         ✅ Removed ${staleIds.length} stale listings from cache`);
            }
            
            // Also remove from undervalued_rent_stabilized table if they exist there
            const { error: resultsError } = await this.supabase
                .from('undervalued_rent_stabilized')
                .delete()
                .in('listing_id', staleIds);
            
            if (resultsError) {
                console.error(`Failed to remove stale listings from results:`, resultsError.message);
            } else {
                console.log(`         ✅ Cleaned up stale listings from results table`);
            }
            
        } catch (error) {
            console.error('Failed to remove stale listings:', error.message);
        }
    }

    /**
     * Utility: Add delay for rate limiting
     */
    async delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    /**
     * Update cache with new listings
     */
    async updateListingCache(newListings) {
        if (newListings.length === 0) return;
        
        try {
            const cacheData = newListings.map(listing => ({
                listing_id: listing.id,
                address: listing.address,
                monthly_rent: listing.price,
                bedrooms: listing.bedrooms,
                bathrooms: listing.bathrooms,
                sqft: listing.sqft,
                description: listing.description,
                neighborhood: listing.neighborhood,
                amenities: listing.amenities,
                listing_url: listing.url,
                listed_at: listing.listedAt,
                cached_at: new Date().toISOString()
            }));
            
            // Upsert to cache table
            const { error } = await this.supabase
                .from('listing_cache')
                .upsert(cacheData, { onConflict: 'listing_id' });
            
            if (error) throw error;
            
        } catch (error) {
            console.error('Cache update failed:', error.message);
        }
    }

    /**
     * STEP 3: Identify rent-stabilized listings using LEGAL INDICATORS ONLY
     */
    async identifyRentStabilizedListings(allListings, stabilizedBuildings) {
        const rentStabilizedListings = [];
        
        for (const listing of allListings) {
            const analysis = this.analyzeRentStabilizationLegal(listing, stabilizedBuildings);
            
            // Only include if high confidence (70%+) of being rent-stabilized
            if (analysis.confidence >= 70) {
                rentStabilizedListings.push({
                    ...listing,
                    rentStabilizedConfidence: analysis.confidence,
                    rentStabilizedFactors: analysis.factors,
                    rentStabilizedMethod: analysis.method
                });
                
                console.log(`     ✅ ${listing.address} - ${analysis.confidence}% (${analysis.method})`);
            }
        }
        
        return rentStabilizedListings;
    }

    /**
     * Analyze rent stabilization using LEGAL indicators only
     */
    analyzeRentStabilizationLegal(listing, stabilizedBuildings) {
        let confidence = 0;
        const factors = [];
        let method = 'circumstantial';
        
        const description = (listing.description || '').toLowerCase();
        
        // Check explicit mentions (highest confidence)
        for (const [keyword, points] of Object.entries(this.RENT_STABILIZED_INDICATORS.explicit)) {
            if (description.includes(keyword)) {
                confidence += points;
                factors.push(`Explicit mention: "${keyword}"`);
                method = 'explicit_mention';
                
                // If explicit mention, we're confident
                if (points >= 85) {
                    return { confidence: Math.min(100, confidence), factors, method };
                }
            }
        }
        
        // Check if building is in DHCR database
        const matchingBuilding = this.findMatchingStabilizedBuilding(listing.address, stabilizedBuildings);
        if (matchingBuilding) {
            confidence += 60; // Base confidence for DHCR registered building
            factors.push(`Building found in DHCR database`);
            method = 'dhcr_registered';
            
            // Add building-specific legal criteria
            const buildingAnalysis = this.analyzeBuildingLegalCriteria(listing, matchingBuilding);
            confidence += buildingAnalysis.confidence;
            factors.push(...buildingAnalysis.factors);
        }
        
        // Check circumstantial evidence (lowest confidence)
        for (const [keyword, points] of Object.entries(this.RENT_STABILIZED_INDICATORS.circumstantial)) {
            if (description.includes(keyword)) {
                confidence += points * 0.5; // Reduce weight for circumstantial
                factors.push(`Circumstantial: "${keyword}"`);
            }
        }
        
        return {
            confidence: Math.min(100, Math.round(confidence)),
            factors,
            method
        };
    }

    /**
     * STEP 4: Analyze undervaluation of rent-stabilized listings ONLY
     */
    async analyzeUndervaluationOfStabilized(rentStabilizedListings, allListings) {
        console.log(`   💰 Analyzing ${rentStabilizedListings.length} rent-stabilized listings for undervaluation...\n`);
        
        const undervaluedStabilized = [];
        
        for (const stabilizedListing of rentStabilizedListings) {
            console.log(`     📍 ${stabilizedListing.address}`);
            
            try {
                // Get market comparables (exclude other rent-stabilized units)
                const marketComparables = this.getMarketRateComparables(stabilizedListing, allListings);
                
                if (marketComparables.length < 5) {
                    console.log(`       ⚠️ Insufficient market comparables (${marketComparables.length})`);
                    continue;
                }
                
                // Run sophisticated undervaluation analysis
                const undervaluationAnalysis = await this.analyzeUndervaluation(
                    stabilizedListing,
                    marketComparables
                );
                
                if (!undervaluationAnalysis.success) {
                    console.log(`       ❌ Undervaluation analysis failed`);
                    continue;
                }
                
                const percentBelowMarket = undervaluationAnalysis.percentBelowMarket;
                console.log(`       📊 ${percentBelowMarket.toFixed(1)}% below market ($${undervaluationAnalysis.estimatedMarketRent.toLocaleString()})`);
                
                // Only include if significantly undervalued
                if (percentBelowMarket >= this.UNDERVALUATION_THRESHOLD) {
                    undervaluedStabilized.push({
                        ...stabilizedListing,
                        estimatedMarketRent: undervaluationAnalysis.estimatedMarketRent,
                        undervaluationPercent: percentBelowMarket,
                        potentialSavings: undervaluationAnalysis.estimatedMarketRent - stabilizedListing.price,
                        undervaluationMethod: undervaluationAnalysis.method,
                        undervaluationConfidence: undervaluationAnalysis.confidence,
                        comparablesUsed: undervaluationAnalysis.comparablesUsed,
                        adjustments: undervaluationAnalysis.adjustments || []
                    });
                    
                    console.log(`       ✅ UNDERVALUED! Savings: $${(undervaluationAnalysis.estimatedMarketRent - stabilizedListing.price).toLocaleString()}/month`);
                } else {
                    console.log(`       📊 Market rate (${percentBelowMarket.toFixed(1)}% below market)`);
                }
                
            } catch (error) {
                console.error(`       ❌ Analysis failed: ${error.message}`);
                continue;
            }
            
            console.log(''); // Empty line
        }
        
        return undervaluedStabilized;
    }

    /**
     * Get market rate comparables (exclude rent-stabilized units)
     */
    getMarketRateComparables(targetListing, allListings) {
        const targetBedrooms = targetListing.bedrooms || 0;
        const targetNeighborhood = targetListing.neighborhood;
        
        return allListings.filter(comp => {
            // Skip target listing
            if (comp.id === targetListing.id) return false;
            
            // Skip listings that appear rent-stabilized
            if (this.appearsRentStabilized(comp)) return false;
            
            // Basic data quality
            if (!comp.price || comp.price <= 0) return false;
            if (comp.bedrooms === null || comp.bedrooms === undefined) return false;
            
            // Bedroom range (±1)
            const bedroomDiff = Math.abs((comp.bedrooms || 0) - targetBedrooms);
            if (bedroomDiff > 1) return false;
            
            // Same or adjacent neighborhood
            if (targetNeighborhood && comp.neighborhood !== targetNeighborhood) {
                const adjacentNeighborhoods = this.getAdjacentNeighborhoods(targetNeighborhood);
                if (!adjacentNeighborhoods.includes(comp.neighborhood)) return false;
            }
            
            // Price sanity check
            if (comp.price < 1500 || comp.price > 15000) return false;
            
            return true;
        });
    }

    /**
     * Quick check if listing appears rent-stabilized (to exclude from market comparables)
     */
    appearsRentStabilized(listing) {
        const description = (listing.description || '').toLowerCase();
        
        // Check for obvious rent-stabilized indicators
        const strongIndicators = [
            'rent stabilized', 'rent-stabilized', 'stabilized unit',
            'dhcr', 'legal rent', 'preferential rent'
        ];
        
        return strongIndicators.some(indicator => description.includes(indicator));
    }

    /**
     * Run sophisticated undervaluation analysis using FULL advanced system from biweekly-rentals
     */
    async analyzeUndervaluation(targetListing, marketComparables) {
        try {
            console.log(`       🧠 Advanced valuation for ${targetListing.address}...`);
            
            // STEP 1: Advanced valuation method selection (from your biweekly-rentals)
            const valuationResult = this.selectAdvancedValuationMethod(targetListing, marketComparables);
            
            if (!valuationResult.success) {
                return { success: false, reason: valuationResult.reason };
            }
            
            console.log(`       📊 Using ${valuationResult.method} with ${valuationResult.comparables.length} comparables`);
            
            // STEP 2: Calculate base market value using advanced methods
            const baseMarketValue = this.calculateAdvancedBaseMarketValue(
                targetListing,
                valuationResult.comparables,
                valuationResult.method
            );
            
            // STEP 3: Apply sophisticated adjustments (amenities, sqft, etc.)
            const adjustedMarketValue = this.applyAdvancedMarketAdjustments(
                targetListing,
                baseMarketValue,
                valuationResult.comparables,
                valuationResult.method
            );
            
            // STEP 4: Calculate confidence score
            const confidence = this.calculateAdvancedConfidenceScore(
                valuationResult.comparables.length,
                valuationResult.method,
                targetListing,
                marketComparables
            );
            
            // STEP 5: Calculate undervaluation
            const estimatedMarketRent = adjustedMarketValue.finalValue;
            const percentBelowMarket = ((estimatedMarketRent - targetListing.price) / estimatedMarketRent) * 100;
            
            console.log(`       💰 Market: $${estimatedMarketRent.toLocaleString()}, Actual: $${targetListing.price.toLocaleString()}`);
            console.log(`       📉 ${percentBelowMarket.toFixed(1)}% below market (${confidence}% confidence)`);
            
            return {
                success: true,
                estimatedMarketRent: estimatedMarketRent,
                baseMarketRent: baseMarketValue.baseValue,
                actualRent: targetListing.price,
                percentBelowMarket: percentBelowMarket,
                method: valuationResult.method,
                confidence: confidence,
                comparablesUsed: valuationResult.comparables.length,
                adjustments: adjustedMarketValue.adjustments,
                totalAdjustments: adjustedMarketValue.totalAdjustments,
                reasoning: this.generateAdvancedValuationReasoning(targetListing, baseMarketValue, adjustedMarketValue, valuationResult)
            };
            
        } catch (error) {
            console.error(`       ❌ Advanced valuation failed: ${error.message}`);
            return { success: false, reason: error.message };
        }
    }

    /**
     * ADVANCED: Select best valuation method (from your biweekly-rentals system)
     */
    selectAdvancedValuationMethod(targetProperty, comparables) {
        const beds = targetProperty.bedrooms || 0;
        const baths = targetProperty.bathrooms || 0;
        
        console.log(`       🔍 Analyzing ${beds}BR/${baths}BA with ${comparables.length} total comparables...`);
        
        // Method 1: Exact bed/bath match with similar amenities (BEST)
        const exactMatches = comparables.filter(comp => 
            comp.bedrooms === beds && 
            Math.abs((comp.bathrooms || 1) - baths) <= 0.5 &&
            this.hasReasonableDataQuality(comp)
        );
        
        if (exactMatches.length >= this.MIN_SAMPLES.EXACT_MATCH) {
            console.log(`       ✅ EXACT_MATCH: ${exactMatches.length} properties with ${beds}BR/${baths}±0.5BA`);
            return {
                success: true,
                method: 'exact_bed_bath_amenity_match',
                comparables: exactMatches
            };
        }

        // Method 2: Same bed/bath count (broader amenity tolerance)
        const bedBathMatches = comparables.filter(comp => 
            comp.bedrooms === beds && 
            (comp.bathrooms || 1) >= (baths - 0.5) && (comp.bathrooms || 1) <= (baths + 1) &&
            this.hasReasonableDataQuality(comp)
        );
        
        if (bedBathMatches.length >= this.MIN_SAMPLES.BED_BATH_SPECIFIC) {
            console.log(`       ✅ BED_BATH_SPECIFIC: ${bedBathMatches.length} properties with ${beds}BR/${baths}±1BA`);
            return {
                success: true,
                method: 'bed_bath_specific_pricing',
                comparables: bedBathMatches
            };
        }

        // Method 3: Same bedroom count (will adjust for bathroom differences)
        const bedMatches = comparables.filter(comp => 
            comp.bedrooms === beds &&
            this.hasReasonableDataQuality(comp)
        );
        
        if (bedMatches.length >= this.MIN_SAMPLES.BED_SPECIFIC) {
            console.log(`       ⚠️ BED_SPECIFIC: ${bedMatches.length} properties with ${beds}BR (will adjust for bath differences)`);
            return {
                success: true,
                method: 'bed_specific_with_adjustments',
                comparables: bedMatches
            };
        }

        // Method 4: Price per sqft fallback (LAST RESORT)
        const sqftComparables = comparables.filter(comp => 
            (comp.sqft || 0) > 0 && (comp.price || 0) > 0 &&
            this.hasReasonableDataQuality(comp)
        );
        
        if (sqftComparables.length >= this.MIN_SAMPLES.PRICE_PER_SQFT_FALLBACK) {
            console.log(`       ⚠️ PRICE_PER_SQFT_FALLBACK: ${sqftComparables.length} properties (least accurate method)`);
            return {
                success: true,
                method: 'price_per_sqft_fallback',
                comparables: sqftComparables
            };
        }

        // Insufficient data
        return {
            success: false,
            reason: `Insufficient comparable data: ${comparables.length} total, need min 3 exact matches for ${beds}BR/${baths}BA`
        };
    }

    /**
     * ADVANCED: Calculate base market value using sophisticated methods
     */
    calculateAdvancedBaseMarketValue(targetProperty, comparables, method) {
        switch (method) {
            case 'exact_bed_bath_amenity_match':
            case 'bed_bath_specific_pricing':
                return this.calculateBedBathBasedValue(targetProperty, comparables);
                
            case 'bed_specific_with_adjustments':
                return this.calculateBedroomBasedValueWithBathAdjustments(targetProperty, comparables);
                
            case 'price_per_sqft_fallback':
                return this.calculateSqftBasedValue(targetProperty, comparables);
                
            default:
                throw new Error(`Unknown valuation method: ${method}`);
        }
    }

    /**
     * Method 1 & 2: Bed/Bath specific pricing (most accurate)
     */
    calculateBedBathBasedValue(targetProperty, comparables) {
        const rents = comparables.map(comp => comp.price).filter(price => price > 0).sort((a, b) => a - b);
        const median = this.calculateMedian(rents);
        
        return {
            baseValue: median,
            method: 'bed_bath_median',
            dataPoints: rents.length,
            rentRange: { min: Math.min(...rents), max: Math.max(...rents) }
        };
    }

    /**
     * Method 3: Bedroom-based with bathroom adjustments
     */
    calculateBedroomBasedValueWithBathAdjustments(targetProperty, comparables) {
        const targetBaths = targetProperty.bathrooms || 1;
        
        // Calculate base rent for this bedroom count
        const rents = comparables.map(comp => comp.price).filter(price => price > 0);
        const medianRent = this.calculateMedian(rents);
        
        // Find typical bathroom count for this bedroom category
        const bathCounts = comparables.map(comp => comp.bathrooms || 1);
        const medianBaths = this.calculateMedian(bathCounts);
        
        // Adjust for bathroom difference
        const bathDifference = targetBaths - medianBaths;
        const bathAdjustment = this.calculateBathroomAdjustment(bathDifference);
        
        return {
            baseValue: medianRent + bathAdjustment,
            method: 'bedroom_based_with_bath_adjustment',
            dataPoints: rents.length,
            bathAdjustment: bathAdjustment,
            medianBaths: medianBaths
        };
    }

    /**
     * Method 4: Price per sqft fallback method
     */
    calculateSqftBasedValue(targetProperty, comparables) {
        const targetSqft = targetProperty.sqft || this.estimateSquareFootage(targetProperty.bedrooms || 0);
        
        // Calculate price per sqft from comparables
        const pricePerSqftValues = comparables
            .filter(comp => (comp.sqft || 0) > 0 && (comp.price || 0) > 0)
            .map(comp => comp.price / comp.sqft);
        
        if (pricePerSqftValues.length === 0) {
            throw new Error('No valid price per sqft data available');
        }
        
        const medianPricePerSqft = this.calculateMedian(pricePerSqftValues);
        const estimatedRent = medianPricePerSqft * targetSqft;
        
        return {
            baseValue: estimatedRent,
            method: 'price_per_sqft',
            dataPoints: pricePerSqftValues.length,
            pricePerSqft: medianPricePerSqft,
            targetSqft: targetSqft
        };
    }

    /**
     * ADVANCED: Apply sophisticated market adjustments (amenities, sqft, etc.)
     */
    applyAdvancedMarketAdjustments(targetProperty, baseValue, comparables, method) {
        const adjustments = [];
        let totalAdjustment = 0;
        const borough = (targetProperty.borough || 'manhattan').toLowerCase();
        
        console.log(`       🔧 Applying adjustments for ${borough}...`);
        
        // STEP 1: Amenity adjustments (sophisticated)
        const targetAmenities = this.extractAmenitiesFromListing(targetProperty);
        const amenityAdjustment = this.calculateAdvancedAmenityAdjustments(targetAmenities, baseValue.baseValue, borough);
        
        if (Math.abs(amenityAdjustment) > 25) { // Only apply significant adjustments
            adjustments.push({
                type: 'amenities',
                amount: amenityAdjustment,
                description: `Amenity adjustments based on ${targetAmenities.length} features`,
                details: targetAmenities
            });
            totalAdjustment += amenityAdjustment;
        }

        // STEP 2: Square footage adjustment (for bed/bath based methods)
        if (method !== 'price_per_sqft_fallback') {
            const sqftAdjustment = this.calculateAdvancedSquareFootageAdjustment(targetProperty, comparables);
            if (Math.abs(sqftAdjustment) > 50) { // Only apply significant adjustments
                adjustments.push({
                    type: 'square_footage',
                    amount: sqftAdjustment,
                    description: `Square footage adjustment (${targetProperty.sqft || 'estimated'} sqft)`
                });
                totalAdjustment += sqftAdjustment;
            }
        }

        // STEP 3: Building quality adjustments
        const buildingQualityAdjustment = this.calculateBuildingQualityAdjustment(targetProperty, comparables);
        if (Math.abs(buildingQualityAdjustment) > 25) {
            adjustments.push({
                type: 'building_quality',
                amount: buildingQualityAdjustment,
                description: 'Building quality adjustment'
            });
            totalAdjustment += buildingQualityAdjustment;
        }

        const finalValue = Math.round(baseValue.baseValue + totalAdjustment);
        
        console.log(`       💰 Base: ${baseValue.baseValue.toLocaleString()} + Adjustments: ${totalAdjustment >= 0 ? '+' : ''}${totalAdjustment.toLocaleString()} = ${finalValue.toLocaleString()}`);
        
        return {
            finalValue: finalValue,
            totalAdjustments: totalAdjustment,
            adjustments: adjustments,
            baseValue: baseValue.baseValue
        };
    }

    /**
     * Calculate advanced amenity adjustments based on listing features
     */
    calculateAdvancedAmenityAdjustments(amenities, baseRent, borough) {
        let adjustment = 0;
        
        // NYC Premium amenities (high-value adds)
        const premiumAmenities = {
            'dishwasher': 100,
            'laundry in unit': 150,
            'washer/dryer': 150,
            'central air': 125,
            'air conditioning': 75,
            'doorman': 200,
            'elevator': 100,
            'gym': 75,
            'fitness center': 75,
            'roof deck': 100,
            'terrace': 150,
            'balcony': 125,
            'parking': 250,
            'garage': 250,
            'storage': 50
        };

        // Check for premium amenities
        for (const amenity of amenities) {
            const amenityLower = amenity.toLowerCase();
            for (const [premium, value] of Object.entries(premiumAmenities)) {
                if (amenityLower.includes(premium)) {
                    adjustment += value;
                    break; // Avoid double counting
                }
            }
        }

        // Borough-specific adjustments
        if (borough === 'manhattan') {
            adjustment *= 1.3; // Manhattan premium
        } else if (borough === 'brooklyn') {
            adjustment *= 1.1; // Brooklyn moderate premium
        }

        return Math.round(adjustment);
    }

    /**
     * Calculate square footage adjustment for bed/bath based methods
     */
    calculateAdvancedSquareFootageAdjustment(targetProperty, comparables) {
        const targetSqft = targetProperty.sqft;
        if (!targetSqft || targetSqft <= 0) return 0;

        // Calculate median sqft of comparables
        const comparableSqfts = comparables
            .map(comp => comp.sqft)
            .filter(sqft => sqft && sqft > 0);
        
        if (comparableSqfts.length === 0) return 0;
        
        const medianSqft = this.calculateMedian(comparableSqfts);
        const sqftDifference = targetSqft - medianSqft;
        
        // Calculate adjustment ($2-4 per sqft difference in NYC)
        const sqftPremium = 3; // $3 per sqft premium/discount
        const adjustment = sqftDifference * sqftPremium;
        
        // Cap adjustments at ±25% of base rent
        const maxAdjustment = targetProperty.price * 0.25;
        return Math.max(-maxAdjustment, Math.min(maxAdjustment, adjustment));
    }

    /**
     * Calculate building quality adjustment based on description analysis
     */
    calculateBuildingQualityAdjustment(targetProperty, comparables) {
        const description = (targetProperty.description || '').toLowerCase();
        let adjustment = 0;

        // Positive indicators
        const positiveIndicators = {
            'luxury': 150,
            'renovated': 100,
            'updated': 75,
            'modern': 50,
            'new': 125,
            'high-end': 100,
            'premium': 75,
            'doorman': 100,
            'concierge': 125,
            'prewar charm': 50
        };

        // Negative indicators
        const negativeIndicators = {
            'needs work': -150,
            'as-is': -100,
            'fixer': -200,
            'handyman': -150,
            'outdated': -75,
            'original': -50,
            'vintage': -25
        };

        // Check positive indicators
        for (const [indicator, value] of Object.entries(positiveIndicators)) {
            if (description.includes(indicator)) {
                adjustment += value;
            }
        }

        // Check negative indicators
        for (const [indicator, value] of Object.entries(negativeIndicators)) {
            if (description.includes(indicator)) {
                adjustment += value; // Already negative
            }
        }

        return Math.round(adjustment);
    }

    /**
     * Calculate bathroom adjustment for bedroom-based method
     */
    calculateBathroomAdjustment(bathDifference) {
        // Each additional bathroom worth ~$150-200/month in NYC
        return Math.round(bathDifference * 175);
    }

    /**
     * Calculate advanced confidence score
     */
    calculateAdvancedConfidenceScore(comparableCount, method, targetProperty, allComparables) {
        let confidence = 0;

        // Base confidence by method
        const methodConfidence = {
            'exact_bed_bath_amenity_match': 85,
            'bed_bath_specific_pricing': 75,
            'bed_specific_with_adjustments': 65,
            'price_per_sqft_fallback': 45
        };
        
        confidence += methodConfidence[method] || 30;

        // Comparable count bonus
        if (comparableCount >= 15) confidence += 10;
        else if (comparableCount >= 10) confidence += 7;
        else if (comparableCount >= 5) confidence += 3;

        // Data quality bonus
        const hasGoodData = targetProperty.sqft && targetProperty.bathrooms;
        if (hasGoodData) confidence += 5;

        // Neighborhood data density bonus
        const neighborhoodComps = allComparables.filter(comp => 
            comp.neighborhood === targetProperty.neighborhood
        ).length;
        
        if (neighborhoodComps >= 20) confidence += 5;
        else if (neighborhoodComps >= 10) confidence += 2;

        return Math.min(100, Math.max(0, Math.round(confidence)));
    }

    /**
     * Extract amenities from listing description and amenities array
     */
    extractAmenitiesFromListing(listing) {
        const amenities = [...(listing.amenities || [])];
        const description = (listing.description || '').toLowerCase();
        
        // Common amenities to look for in description
        const amenityKeywords = [
            'dishwasher', 'laundry', 'washer', 'dryer', 'air conditioning',
            'doorman', 'elevator', 'gym', 'fitness', 'roof', 'terrace',
            'balcony', 'parking', 'garage', 'storage'
        ];
        
        for (const keyword of amenityKeywords) {
            if (description.includes(keyword) && !amenities.some(a => a.toLowerCase().includes(keyword))) {
                amenities.push(keyword);
            }
        }
        
        return amenities;
    }

    /**
     * Check if comparable has reasonable data quality
     */
    hasReasonableDataQuality(comparable) {
        // Must have basic data
        if (!comparable.price || comparable.price <= 0) return false;
        if (comparable.bedrooms === null || comparable.bedrooms === undefined) return false;
        
        // Price sanity check for NYC
        if (comparable.price < 1500 || comparable.price > 15000) return false;
        
        return true;
    }

    /**
     * Estimate square footage based on bedroom count (NYC averages)
     */
    estimateSquareFootage(bedrooms) {
        const estimates = {
            0: 500,  // Studio
            1: 700,  // 1BR
            2: 1000, // 2BR
            3: 1300, // 3BR
            4: 1600  // 4BR+
        };
        
        return estimates[bedrooms] || estimates[4];
    }

    /**
     * Calculate median of array
     */
    calculateMedian(arr) {
        const sorted = [...arr].sort((a, b) => a - b);
        const mid = Math.floor(sorted.length / 2);
        return sorted.length % 2 !== 0 
            ? sorted[mid] 
            : (sorted[mid - 1] + sorted[mid]) / 2;
    }

    /**
     * Generate advanced valuation reasoning
     */
    generateAdvancedValuationReasoning(targetListing, baseValue, adjustedValue, valuationResult) {
        const reasoning = [];
        
        reasoning.push(`Used ${valuationResult.method} with ${valuationResult.comparables.length} comparables`);
        reasoning.push(`Base market value: ${baseValue.baseValue.toLocaleString()}`);
        
        if (adjustedValue.adjustments.length > 0) {
            reasoning.push(`Applied ${adjustedValue.adjustments.length} adjustments:`);
            adjustedValue.adjustments.forEach(adj => {
                reasoning.push(`  - ${adj.description}: ${adj.amount >= 0 ? '+' : ''}${adj.amount}`);
            });
        }
        
        reasoning.push(`Final estimated market rent: ${adjustedValue.finalValue.toLocaleString()}`);
        
        return reasoning.join('\n');
    }

    /**
     * Load rent-stabilized buildings database (with DHCR file parsing)
     */
    async loadRentStabilizedBuildings() {
        try {
            // First try to load from database
            const { data, error } = await this.supabase
                .from('rent_stabilized_buildings')
                .select('*');
            
            if (error) throw error;
            
            // If database is empty, try to parse DHCR files
            if (!data || data.length === 0) {
                console.log('📁 No buildings in database, checking for DHCR files...');
                const parsedBuildings = await this.parseDHCRFiles();
                
                if (parsedBuildings.length > 0) {
                    await this.saveDHCRBuildingsToDatabase(parsedBuildings);
                    return parsedBuildings;
                }
            }
            
            return data || [];
            
        } catch (error) {
            console.error('Failed to load stabilized buildings:', error.message);
            return [];
        }
    }

    /**
     * Parse DHCR files from data/dhcr/ directory
     */
    async parseDHCRFiles() {
        const fs = require('fs').promises;
        const path = require('path');
        
        try {
            const dhcrDir = path.join(process.cwd(), 'data', 'dhcr');
            const buildings = [];
            
            console.log(`📂 Scanning DHCR directory: ${dhcrDir}`);
            
            // Check if directory exists
            try {
                await fs.access(dhcrDir);
            } catch (error) {
                console.log('📁 Creating data/dhcr directory...');
                await fs.mkdir(dhcrDir, { recursive: true });
                console.log('💡 Place DHCR files in data/dhcr/ directory');
                return [];
            }
            
            const files = await fs.readdir(dhcrDir);
            console.log(`📄 Found ${files.length} files in DHCR directory`);
            
            for (const file of files) {
                const filePath = path.join(dhcrDir, file);
                const ext = path.extname(file).toLowerCase();
                
                try {
                    console.log(`📖 Processing ${file}...`);
                    
                    if (ext === '.csv') {
                        const csvBuildings = await this.parseDHCRCSV(filePath);
                        buildings.push(...csvBuildings);
                        console.log(`   ✅ Parsed ${csvBuildings.length} buildings from CSV`);
                        
                    } else if (ext === '.pdf') {
                        const pdfBuildings = await this.parseDHCRPDF(filePath);
                        buildings.push(...pdfBuildings);
                        console.log(`   ✅ Parsed ${pdfBuildings.length} buildings from PDF`);
                        
                    } else if (ext === '.xlsx' || ext === '.xls') {
                        const excelBuildings = await this.parseDHCRExcel(filePath);
                        buildings.push(...excelBuildings);
                        console.log(`   ✅ Parsed ${excelBuildings.length} buildings from Excel`);
                        
                    } else {
                        console.log(`   ⚠️ Skipping unsupported file type: ${ext}`);
                    }
                    
                } catch (error) {
                    console.error(`   ❌ Error parsing ${file}:`, error.message);
                    continue;
                }
            }
            
            // Remove duplicates by address
            const uniqueBuildings = this.deduplicateBuildings(buildings);
            console.log(`🏢 Total unique buildings parsed: ${uniqueBuildings.length}`);
            
            return uniqueBuildings;
            
        } catch (error) {
            console.error('Failed to parse DHCR files:', error.message);
            return [];
        }
    }

    /**
     * Parse DHCR CSV file
     */
    async parseDHCRCSV(filePath) {
        const fs = require('fs').promises;
        
        try {
            const csvContent = await fs.readFile(filePath, 'utf8');
            
            const parsed = Papa.parse(csvContent, {
                header: true,
                skipEmptyLines: true,
                transformHeader: (header) => header.toLowerCase().replace(/[^a-z0-9]/g, '_')
            });
            
            if (parsed.errors.length > 0) {
                console.warn(`   ⚠️ CSV parsing warnings:`, parsed.errors.slice(0, 3));
            }
            
            return parsed.data.map(row => this.normalizeBuildingData(row, 'csv'));
            
        } catch (error) {
            console.error(`Failed to parse CSV ${filePath}:`, error.message);
            return [];
        }
    }

    /**
     * Parse DHCR PDF file
     */
    async parseDHCRPDF(filePath) {
        const fs = require('fs').promises;
        
        try {
            const pdfBuffer = await fs.readFile(filePath);
            const data = await pdf(pdfBuffer);
            
            // Extract building data from PDF text using patterns
            const buildings = this.extractBuildingsFromPDFText(data.text);
            
            return buildings.map(building => this.normalizeBuildingData(building, 'pdf'));
            
        } catch (error) {
            console.error(`Failed to parse PDF ${filePath}:`, error.message);
            return [];
        }
    }

    /**
     * Parse DHCR Excel file
     */
    async parseDHCRExcel(filePath) {
        const fs = require('fs').promises;
        
        try {
            const workbook = XLSX.readFile(filePath);
            const sheetName = workbook.SheetNames[0];
            const worksheet = workbook.Sheets[sheetName];
            
            const jsonData = XLSX.utils.sheet_to_json(worksheet, {
                header: 1,
                defval: ''
            });
            
            // Convert to objects with headers
            if (jsonData.length < 2) return [];
            
            const headers = jsonData[0].map(h => 
                h.toString().toLowerCase().replace(/[^a-z0-9]/g, '_')
            );
            
            const buildings = jsonData.slice(1).map(row => {
                const building = {};
                headers.forEach((header, index) => {
                    building[header] = row[index] || '';
                });
                return this.normalizeBuildingData(building, 'excel');
            });
            
            return buildings;
            
        } catch (error) {
            console.error(`Failed to parse Excel ${filePath}:`, error.message);
            return [];
        }
    }

    /**
     * Extract building data from PDF text using patterns
     */
    extractBuildingsFromPDFText(text) {
        const buildings = [];
        const lines = text.split('\n');
        
        // Common DHCR PDF patterns
        const addressPattern = /^\s*(\d+)\s+([A-Z\s]+(?:STREET|ST|AVENUE|AVE|ROAD|RD|PLACE|PL|BOULEVARD|BLVD))/i;
        const boroughPattern = /(MANHATTAN|BROOKLYN|QUEENS|BRONX|STATEN ISLAND)/i;
        
        let currentBuilding = null;
        
        for (const line of lines) {
            const trimmedLine = line.trim();
            if (!trimmedLine) continue;
            
            // Try to match address pattern
            const addressMatch = trimmedLine.match(addressPattern);
            if (addressMatch) {
                // Save previous building if exists
                if (currentBuilding && currentBuilding.address) {
                    buildings.push(currentBuilding);
                }
                
                // Start new building
                currentBuilding = {
                    address: `${addressMatch[1]} ${addressMatch[2]}`.trim(),
                    house_number: addressMatch[1],
                    street_name: addressMatch[2].trim()
                };
                
                // Check if borough is on same line
                const boroughMatch = trimmedLine.match(boroughPattern);
                if (boroughMatch) {
                    currentBuilding.borough = boroughMatch[1];
                }
            } else if (currentBuilding) {
                // Try to extract additional info for current building
                const boroughMatch = trimmedLine.match(boroughPattern);
                if (boroughMatch && !currentBuilding.borough) {
                    currentBuilding.borough = boroughMatch[1];
                }
                
                // Look for unit count, year built, etc.
                const unitMatch = trimmedLine.match(/(\d+)\s*UNITS?/i);
                if (unitMatch) {
                    currentBuilding.total_units = parseInt(unitMatch[1]);
                }
                
                const yearMatch = trimmedLine.match(/BUILT[:\s]*(\d{4})/i);
                if (yearMatch) {
                    currentBuilding.year_built = parseInt(yearMatch[1]);
                }
            }
        }
        
        // Don't forget the last building
        if (currentBuilding && currentBuilding.address) {
            buildings.push(currentBuilding);
        }
        
        return buildings;
    }

    /**
     * Convert DHCR county code to borough name (CORRECTED)
     */
    convertCountyCodeToBorough(countyCode) {
        const countyMap = {
            '60': 'BRONX',
            '61': 'BROOKLYN', 
            '62': 'MANHATTAN',
            '63': 'QUEENS'
            // Staten Island not included yet
        };
        
        return countyMap[countyCode?.toString()] || null;
    }

    /**
     * Normalize building data from different sources (UPDATED for real DHCR format)
     */
    normalizeBuildingData(rawData, source) {
        const normalized = {
            source: source,
            parsed_at: new Date().toISOString()
        };
        
        if (source === 'pdf' || source === 'csv') {
            // Handle DHCR-specific format with ALL fields
            
            // Address components
            const zip = rawData.ZIP || rawData.zip;
            const houseNumber1 = rawData.BLDGNO1 || rawData.bldgno1;
            const street1 = rawData.STREET1 || rawData.street1;
            const suffix1 = rawData.STSUFX1 || rawData.stsufx1;
            const houseNumber2 = rawData.BLDGNO2 || rawData.bldgno2;
            const street2 = rawData.STREET2 || rawData.street2;
            const suffix2 = rawData.STSUFX2 || rawData.stsufx2;
            
            // Build primary address
            if (houseNumber1 && street1) {
                let address = houseNumber1;
                if (street1) address += ' ' + street1;
                if (suffix1) address += ' ' + suffix1;
                
                normalized.address = address;
                normalized.house_number = houseNumber1;
                normalized.street_name = street1 + (suffix1 ? ' ' + suffix1 : '');
            }
            
            // Build secondary address if exists
            if (houseNumber2 && street2) {
                let secondaryAddress = houseNumber2;
                if (street2) secondaryAddress += ' ' + street2;
                if (suffix2) secondaryAddress += ' ' + suffix2;
                
                normalized.secondary_address = secondaryAddress;
                normalized.secondary_house_number = houseNumber2;
                normalized.secondary_street_name = street2 + (suffix2 ? ' ' + suffix2 : '');
            }
            
            // Convert county code to borough
            const countyCode = rawData.COUNTY || rawData.county;
            normalized.borough = this.convertCountyCodeToBorough(countyCode);
            normalized.county_code = countyCode;
            
            // Basic info
            normalized.zip_code = zip;
            normalized.city = rawData.CITY || rawData.city;
            
            // Status fields (capture ALL of them)
            normalized.status1 = rawData.STATUS1 || rawData.status1;
            normalized.status2 = rawData.STATUS2 || rawData.status2;
            normalized.status3 = rawData.STATUS3 || rawData.status3;
            
            // Tax lot information
            normalized.block = rawData.BLOCK || rawData.block;
            normalized.lot = rawData.LOT || rawData.lot;
            
            // Set DHCR registration to true (since it's in the DHCR file)
            normalized.dhcr_registered = true;
            
            // Try to extract unit count from status fields
            normalized.total_units = this.extractUnitCount(rawData);
            
            // Try to extract year built from status fields
            normalized.year_built = this.extractYearBuilt(rawData);
            
            // Store building type
            normalized.building_type = normalized.status1;
            
        } else {
            // Handle other sources (Excel, manual input, etc.)
            // Keep existing logic for backward compatibility
            const fieldMappings = {
                address: ['address', 'building_address', 'street_address', 'full_address'],
                house_number: ['house_number', 'house_num', 'building_number', 'bldg_num'],
                street_name: ['street_name', 'street', 'street_address'],
                borough: ['borough', 'boro', 'county'],
                zip_code: ['zip_code', 'zip', 'postal_code'],
                total_units: ['total_units', 'units', 'unit_count', 'number_of_units'],
                year_built: ['year_built', 'built_year', 'construction_year'],
                dhcr_registered: ['dhcr_registered', 'registered', 'dhcr_status'],
                building_id: ['building_id', 'bldg_id', 'dhcr_id'],
                registration_year: ['registration_year', 'reg_year']
            };
            
            // Apply field mappings for non-DHCR sources
            for (const [standardField, possibleFields] of Object.entries(fieldMappings)) {
                for (const field of possibleFields) {
                    if (rawData[field] !== undefined && rawData[field] !== '') {
                        let value = rawData[field];
                        
                        if (['total_units', 'year_built'].includes(standardField)) {
                            value = parseInt(value) || 0;
                        } else if (standardField === 'dhcr_registered') {
                            value = ['true', 'yes', '1', 'registered'].includes(
                                value.toString().toLowerCase()
                            );
                        } else if (typeof value === 'string') {
                            value = value.trim();
                        }
                        
                        normalized[standardField] = value;
                        break;
                    }
                }
            }
        }
        
        // Ensure we have an address
        if (!normalized.address && normalized.house_number && normalized.street_name) {
            normalized.address = `${normalized.house_number} ${normalized.street_name}`;
        }
        
        // Set default DHCR registration
        if (normalized.dhcr_registered === undefined) {
            normalized.dhcr_registered = true; // Assume true if in DHCR file
        }
        
        return normalized;
    }

    /**
     * Extract unit count from DHCR status fields
     */
    extractUnitCount(rawData) {
        // Look for unit count in status fields or other places
        const statusFields = [
            rawData.STATUS1, rawData.STATUS2, rawData.STATUS3,
            rawData.status1, rawData.status2, rawData.status3
        ].filter(Boolean);
        
        for (const status of statusFields) {
            const unitMatch = status.toString().match(/(\d+)\s*UNIT/i);
            if (unitMatch) {
                return parseInt(unitMatch[1]);
            }
        }
        
        return null;
    }

    /**
     * Extract year built from DHCR status fields
     */
    extractYearBuilt(rawData) {
        // Look for year in status fields
        const statusFields = [
            rawData.STATUS1, rawData.STATUS2, rawData.STATUS3,
            rawData.status1, rawData.status2, rawData.status3
        ].filter(Boolean);
        
        for (const status of statusFields) {
            const yearMatch = status.toString().match(/\b(19|20)\d{2}\b/);
            if (yearMatch) {
                return parseInt(yearMatch[0]);
            }
        }
        
        return null;
    }

    /**
     * Remove duplicate buildings by address
     */
    deduplicateBuildings(buildings) {
        const seen = new Map();
        const unique = [];
        
        for (const building of buildings) {
            if (!building.address) continue;
            
            const normalizedAddress = this.normalizeAddress(building.address);
            const key = `${normalizedAddress}_${building.borough || ''}`;
            
            if (!seen.has(key)) {
                seen.set(key, true);
                unique.push(building);
            }
        }
        
        return unique;
    }

    /**
     * Save parsed DHCR buildings to database
     */
    async saveDHCRBuildingsToDatabase(buildings) {
        if (buildings.length === 0) return;
        
        try {
            console.log(`💾 Saving ${buildings.length} DHCR buildings to database...`);
            
            // Insert in batches to avoid size limits
            const batchSize = 500;
            let saved = 0;
            
            for (let i = 0; i < buildings.length; i += batchSize) {
                const batch = buildings.slice(i, i + batchSize);
                
                const { error } = await this.supabase
                    .from('rent_stabilized_buildings')
                    .upsert(batch, { 
                        onConflict: 'address,borough',
                        ignoreDuplicates: false 
                    });
                
                if (error) {
                    console.error(`Batch ${Math.floor(i/batchSize) + 1} failed:`, error.message);
                    continue;
                }
                
                saved += batch.length;
                console.log(`   ✅ Saved batch ${Math.floor(i/batchSize) + 1}: ${saved}/${buildings.length} buildings`);
            }
            
            console.log(`🎉 Successfully saved ${saved} DHCR buildings to database`);
            
        } catch (error) {
            console.error('Failed to save DHCR buildings:', error.message);
        }
    }

    /**
     * Find matching stabilized building by address
     */
    findMatchingStabilizedBuilding(address, stabilizedBuildings) {
        if (!address || stabilizedBuildings.length === 0) return null;
        
        const normalizedAddress = this.normalizeAddress(address);
        
        return stabilizedBuildings.find(building => {
            const buildingAddress = this.normalizeAddress(building.address || '');
            return buildingAddress && normalizedAddress.includes(buildingAddress.substring(0, 10));
        });
    }

    /**
     * Analyze building legal criteria for rent stabilization
     */
    analyzeBuildingLegalCriteria(listing, building) {
        let confidence = 0;
        const factors = [];
        
        // Building age criteria
        const buildYear = building.year_built || this.extractYearFromDescription(listing.description);
        if (buildYear) {
            if (buildYear < 1947) {
                confidence += 20;
                factors.push('Pre-1947 building (pre-war)');
            } else if (buildYear <= 1973) {
                confidence += 15;
                factors.push('1947-1973 building (post-war regulated era)');
            }
        }
        
        // Unit count criteria
        if (building.total_units >= 6) {
            confidence += 15;
            factors.push('6+ units (meets minimum for stabilization)');
        }
        
        // DHCR registration status
        if (building.dhcr_registered) {
            confidence += 25;
            factors.push('DHCR registered building');
        }
        
        return { confidence, factors };
    }

    /**
     * Get adjacent neighborhoods for comparison
     */
    getAdjacentNeighborhoods(neighborhood) {
        const adjacencies = {
            'east-village': ['lower-east-side', 'greenwich-village', 'noho'],
            'lower-east-side': ['east-village', 'chinatown', 'two-bridges'],
            'chinatown': ['lower-east-side', 'tribeca', 'financial-district'],
            'greenwich-village': ['east-village', 'west-village', 'noho'],
            'west-village': ['greenwich-village', 'meatpacking-district', 'chelsea'],
            'chelsea': ['west-village', 'flatiron', 'midtown-west'],
            'murray-hill': ['midtown-east', 'gramercy', 'kips-bay'],
            'upper-east-side': ['midtown-east', 'yorkville'],
            'upper-west-side': ['midtown-west', 'morningside-heights']
        };
        
        return adjacencies[neighborhood] || [];
    }

    /**
     * Normalize address for matching
     */
    normalizeAddress(address) {
        return address.toLowerCase()
            .replace(/[^\w\s]/g, ' ')
            .replace(/\s+/g, ' ')
            .trim();
    }

    /**
     * Extract year from description text
     */
    extractYearFromDescription(description) {
        if (!description) return null;
        
        const yearMatch = description.match(/\b(19|20)\d{2}\b/);
        return yearMatch ? parseInt(yearMatch[0]) : null;
    }

    /**
     * Save results to database
     */
    async saveResults(undervaluedStabilized) {
        if (undervaluedStabilized.length === 0) return;
        
        try {
            const resultsData = undervaluedStabilized.map(listing => ({
                listing_id: listing.id,
                address: listing.address,
                neighborhood: listing.neighborhood,
                monthly_rent: listing.price,
                estimated_market_rent: listing.estimatedMarketRent,
                undervaluation_percent: listing.undervaluationPercent,
                potential_monthly_savings: listing.potentialSavings,
                bedrooms: listing.bedrooms,
                bathrooms: listing.bathrooms,
                sqft: listing.sqft,
                rent_stabilized_confidence: listing.rentStabilizedConfidence,
                rent_stabilized_method: listing.rentStabilizedMethod,
                undervaluation_method: listing.undervaluationMethod,
                undervaluation_confidence: listing.undervaluationConfidence,
                comparables_used: listing.comparablesUsed,
                listing_url: listing.url,
                analyzed_at: new Date().toISOString()
            }));
            
            const { error } = await this.supabase
                .from('undervalued_rent_stabilized')
                .upsert(resultsData, { onConflict: 'listing_id' });
            
            if (error) throw error;
            
            console.log(`   ✅ Saved ${resultsData.length} results to database`);
            
        } catch (error) {
            console.error('Failed to save results:', error.message);
        }
    }

    /**
     * Generate final report
     */
    generateFinalReport(undervaluedStabilized) {
        console.log('\n🎉 RENT-STABILIZED UNDERVALUED LISTINGS REPORT');
        console.log('='.repeat(60));
        
        if (undervaluedStabilized.length === 0) {
            console.log('❌ No undervalued rent-stabilized listings found');
            console.log('💡 This is normal - true undervalued rent-stabilized units are rare');
            return;
        }
        
        console.log(`✅ Found ${undervaluedStabilized.length} undervalued rent-stabilized listings!\n`);
        
        // Sort by savings potential
        const sorted = undervaluedStabilized.sort((a, b) => b.potentialSavings - a.potentialSavings);
        
        sorted.forEach((listing, index) => {
            console.log(`${index + 1}. ${listing.address}`);
            console.log(`   💰 Rent: ${listing.price.toLocaleString()}/month`);
            console.log(`   📊 Market: ${listing.estimatedMarketRent.toLocaleString()}/month`);
            console.log(`   💵 Savings: ${listing.potentialSavings.toLocaleString()}/month (${(listing.potentialSavings * 12).toLocaleString()}/year)`);
            console.log(`   📉 Discount: ${listing.undervaluationPercent.toFixed(1)}% below market`);
            console.log(`   🏠 Layout: ${listing.bedrooms}BR/${listing.bathrooms}BA${listing.sqft ? ` (${listing.sqft} sqft)` : ''}`);
            console.log(`   ⚖️ Stabilized: ${listing.rentStabilizedConfidence}% confidence (${listing.rentStabilizedMethod})`);
            console.log(`   🎯 Valuation: ${listing.undervaluationConfidence}% confidence (${listing.undervaluationMethod})`);
            console.log(`   🔗 ${listing.url}\n`);
        });
        
        // Summary statistics
        const totalSavings = sorted.reduce((sum, listing) => sum + listing.potentialSavings, 0);
        const avgSavings = totalSavings / sorted.length;
        const avgDiscount = sorted.reduce((sum, listing) => sum + listing.undervaluationPercent, 0) / sorted.length;
        
        console.log('📊 SUMMARY STATISTICS');
        console.log(`   • Total monthly savings across all listings: ${totalSavings.toLocaleString()}`);
        console.log(`   • Average monthly savings per listing: ${avgSavings.toLocaleString()}`);
        console.log(`   • Average discount from market rate: ${avgDiscount.toFixed(1)}%`);
        console.log(`   • Best deal: ${sorted[0].address} (${sorted[0].undervaluationPercent.toFixed(1)}% below market)`);
        
        console.log('\n💡 NEXT STEPS');
        console.log('   1. Contact landlords/brokers immediately - these deals move fast');
        console.log('   2. Verify rent stabilization status with landlord/DHCR');
        console.log('   3. Confirm all amenities and condition during viewing');
        console.log('   4. Have paperwork ready for quick application');
    }
}

// Export for use in other modules
module.exports = RentStabilizedUndervaluedDetector;

// Main execution function
async function main() {
    const detector = new RentStabilizedUndervaluedDetector();
    
    try {
        const results = await detector.findUndervaluedRentStabilizedListings({
            neighborhoods: ['east-village', 'lower-east-side', 'chinatown', 'greenwich-village'],
            maxListingsPerNeighborhood: 150,
            testMode: false
        });
        
        console.log('\n🎉 Analysis complete!');
        console.log(`📊 Check your Supabase 'undervalued_rent_stabilized' table for ${results.undervaluedStabilizedFound} deals`);
        
        return results;
        
    } catch (error) {
        console.error('💥 Analysis failed:', error);
        process.exit(1);
    }
}

// Run if executed directly
if (require.main === module) {
    main().catch(error => {
        console.error('💥 Rent-stabilized undervalued detector crashed:', error);
        process.exit(1);
    });
}
