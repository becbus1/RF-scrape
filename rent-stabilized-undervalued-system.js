// rent-stabilized-undervalued-system.js
// PRODUCTION-GRADE: Find rent-stabilized listings + save ALL regardless of market position
// STREAMLINED: Removed DHCR parsing complexity, added comprehensive listing caching

require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');

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

        // STEP 2: Market classification thresholds (from .env)
        this.UNDERVALUATION_THRESHOLD = parseInt(process.env.UNDERVALUATION_THRESHOLD) || 15;
        this.MODERATE_UNDERVALUATION_THRESHOLD = 5;
        this.MARKET_RATE_THRESHOLD = 5;
        this.OVERVALUED_THRESHOLD = -5;
        
        this.VALUATION_METHODS = {
            EXACT_MATCH: 'exact_bed_bath_amenity_match',
            BED_BATH_SPECIFIC: 'bed_bath_specific_pricing',
            BED_SPECIFIC: 'bed_specific_with_adjustments',
            PRICE_PER_SQFT_FALLBACK: 'price_per_sqft_fallback'
        };

        // Minimum sample sizes for each method
        this.MIN_SAMPLES = {
            EXACT_MATCH: 3,
            BED_BATH_SPECIFIC: 8,
            BED_SPECIFIC: 12,
            PRICE_PER_SQFT_FALLBACK: 20
        };

        // API settings from .env
        this.MAX_LISTINGS_PER_NEIGHBORHOOD = parseInt(process.env.MAX_LISTINGS_PER_NEIGHBORHOOD) || 2000;
        this.MAX_LISTINGS_PER_FETCH = parseInt(process.env.MAX_LISTINGS_PER_FETCH) || 500;
        this.PAGINATION_OFFSET_INCREMENT = parseInt(process.env.PAGINATION_OFFSET_INCREMENT) || 500;
    }

    /**
     * MAIN FUNCTION: Find ALL rent-stabilized listings and save them
     */
    async findUndervaluedRentStabilizedListings(options = {}) {
        console.log('🏠 Finding ALL rent-stabilized listings (saving regardless of market position)...\n');

        const {
            neighborhoods = ['east-village', 'lower-east-side', 'chinatown'],
            maxListingsPerNeighborhood = this.MAX_LISTINGS_PER_NEIGHBORHOOD,
            testMode = false
        } = options;

        try {
            // Step 1: Get ALL listings with comprehensive caching
            console.log('📋 Step 1: Fetching all listings with comprehensive caching...');
            const allListings = await this.getAllListingsWithComprehensiveCaching(neighborhoods, maxListingsPerNeighborhood);
            console.log(`   ✅ Total listings: ${allListings.length}\n`);

            // Step 2: Load rent-stabilized buildings (simple database load)
            console.log('🏢 Step 2: Loading rent-stabilized buildings from database...');
            const stabilizedBuildings = await this.loadRentStabilizedBuildingsSimple();
            console.log(`   ✅ Stabilized buildings: ${stabilizedBuildings.length}\n`);

            // Step 3: Find rent-stabilized listings
            console.log('⚖️ Step 3: Identifying rent-stabilized listings...');
            const rentStabilizedListings = await this.identifyRentStabilizedListings(
                allListings, 
                stabilizedBuildings
            );
            console.log(`   ✅ Rent-stabilized found: ${rentStabilizedListings.length}\n`);

            // Step 4: Analyze ALL rent-stabilized listings (save regardless of market position)
            console.log('💰 Step 4: Analyzing ALL rent-stabilized listings...');
            const analyzedStabilized = await this.analyzeAllRentStabilizedListings(
                rentStabilizedListings,
                allListings
            );
            console.log(`   ✅ Analyzed rent-stabilized: ${analyzedStabilized.length}\n`);

            // Step 5: Save ALL results
            await this.saveAllResults(analyzedStabilized);
            this.generateFinalReport(analyzedStabilized);

            return {
                totalListingsScanned: allListings.length,
                rentStabilizedFound: rentStabilizedListings.length,
                allRentStabilizedSaved: analyzedStabilized.length,
                results: analyzedStabilized.sort((a, b) => b.undervaluationPercent - a.undervaluationPercent)
            };

        } catch (error) {
            console.error('💥 Analysis failed:', error.message);
            throw error;
        }
    }

    /**
     * IMPROVED: Get all listings with comprehensive caching for ALL listings
     */
    async getAllListingsWithComprehensiveCaching(neighborhoods, maxPerNeighborhood) {
        const allListings = [];
        
        for (const neighborhood of neighborhoods) {
            console.log(`   📍 Fetching ${neighborhood} with comprehensive caching...`);
            
            try {
                // Get all listing IDs for this neighborhood (with pagination)
                const allListingIds = await this.fetchAllListingIdsForNeighborhood(neighborhood, maxPerNeighborhood);
                console.log(`     📋 Found ${allListingIds.length} total listing IDs`);
                
                if (allListingIds.length === 0) {
                    console.log(`     ⚠️ No listings found in ${neighborhood}`);
                    continue;
                }
                
                // Check which listings we already have cached
                const cachedListings = await this.getCachedListingsByIds(allListingIds);
                const cachedIds = new Set(cachedListings.map(l => l.id));
                const newListingIds = allListingIds.filter(id => !cachedIds.has(id));
                
                console.log(`     💾 Cache hit: ${cachedListings.length} listings already cached`);
                console.log(`     🆕 New listings to fetch: ${newListingIds.length}`);
                
                // Fetch detailed data for new listings only
                const newListings = await this.fetchDetailedListingData(newListingIds, neighborhood);
                
                // Cache the new listings
                if (newListings.length > 0) {
                    await this.cacheAllListings(newListings);
                }
                
                // Combine cached + new
                const neighborhoodListings = [...cachedListings, ...newListings];
                allListings.push(...neighborhoodListings);
                
                const efficiency = allListingIds.length > 0 ? 
                    ((allListingIds.length - newListingIds.length) / allListingIds.length * 100).toFixed(1) : 0;
                
                console.log(`     ✅ Total: ${neighborhoodListings.length} listings (${efficiency}% cache efficiency)`);
                
            } catch (error) {
                console.error(`     ❌ Failed to fetch ${neighborhood}:`, error.message);
                continue;
            }
        }
        
        return allListings;
    }

    /**
     * NEW: Fetch ALL listing IDs for neighborhood with pagination
     */
    async fetchAllListingIdsForNeighborhood(neighborhood, maxListings) {
        const allIds = [];
        let offset = 0;
        const limit = this.MAX_LISTINGS_PER_FETCH;
        
        try {
            const rapidApiKey = process.env.RAPIDAPI_KEY;
            if (!rapidApiKey) {
                console.log(`       ⚠️ No RAPIDAPI_KEY found, cannot fetch from StreetEasy`);
                return [];
            }

            console.log(`       🌐 Fetching listing IDs with pagination (max: ${maxListings})...`);
            
            while (offset < maxListings) {
                const currentLimit = Math.min(limit, maxListings - offset);
                
                console.log(`       📡 API call: offset=${offset}, limit=${currentLimit}`);
                
                const response = await axios.get('https://streeteasy1.p.rapidapi.com/rentals/search', {
                    headers: {
                        'X-RapidAPI-Key': rapidApiKey,
                        'X-RapidAPI-Host': 'streeteasy1.p.rapidapi.com'
                    },
                    params: {
                        neighborhood: neighborhood,
                        limit: currentLimit,
                        offset: offset,
                        format: 'json'
                    },
                    timeout: 30000
                });

                if (response.data && response.data.rentals && response.data.rentals.length > 0) {
                    const batchIds = response.data.rentals.map(rental => rental.id?.toString()).filter(Boolean);
                    allIds.push(...batchIds);
                    
                    console.log(`       ✅ Batch: ${batchIds.length} IDs (total: ${allIds.length})`);
                    
                    // If we got fewer results than requested, we've hit the end
                    if (response.data.rentals.length < currentLimit) {
                        console.log(`       🏁 Reached end of results for ${neighborhood}`);
                        break;
                    }
                } else {
                    console.log(`       🏁 No more results for ${neighborhood}`);
                    break;
                }
                
                offset += this.PAGINATION_OFFSET_INCREMENT;
                
                // Rate limiting
                await this.delay(2000);
            }
            
            console.log(`       🎉 Total IDs collected: ${allIds.length}`);
            return [...new Set(allIds)]; // Remove duplicates
            
        } catch (error) {
            console.error(`       ❌ Failed to fetch listing IDs for ${neighborhood}:`, error.message);
            return allIds; // Return what we got so far
        }
    }

    /**
     * NEW: Get cached listings by specific IDs
     */
    async getCachedListingsByIds(listingIds) {
        if (listingIds.length === 0) return [];
        
        try {
            // Try comprehensive_listing_cache first (new table for ALL listings)
            const { data: comprehensive, error: comprehensiveError } = await this.supabase
                .from('comprehensive_listing_cache')
                .select('*')
                .in('listing_id', listingIds);
            
            if (!comprehensiveError && comprehensive && comprehensive.length > 0) {
                return comprehensive.map(row => ({
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
                    source: 'comprehensive_cache'
                }));
            }
            
            // Fallback to existing caches
            const { data: rental, error: rentalError } = await this.supabase
                .from('rental_market_cache')
                .select('*')
                .in('listing_id', listingIds);
            
            if (!rentalError && rental && rental.length > 0) {
                return rental.map(row => ({
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
                }));
            }
            
            // Final fallback to listing_cache
            const { data: basic, error: basicError } = await this.supabase
                .from('listing_cache')
                .select('*')
                .in('listing_id', listingIds);
            
            if (!basicError && basic && basic.length > 0) {
                return basic.map(row => ({
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
                }));
            }
            
            return [];
            
        } catch (error) {
            console.error('Failed to get cached listings:', error.message);
            return [];
        }
    }

    /**
     * NEW: Fetch detailed listing data for specific IDs
     */
    async fetchDetailedListingData(listingIds, neighborhood) {
        if (listingIds.length === 0) return [];
        
        const detailedListings = [];
        const batchSize = 20; // Fetch in smaller batches to avoid timeouts
        
        try {
            console.log(`         📚 Fetching detailed data for ${listingIds.length} new listings...`);
            
            for (let i = 0; i < listingIds.length; i += batchSize) {
                const batch = listingIds.slice(i, i + batchSize);
                
                for (const listingId of batch) {
                    try {
                        const detailedListing = await this.fetchSingleListingDetails(listingId, neighborhood);
                        if (detailedListing) {
                            detailedListings.push(detailedListing);
                        }
                        
                        // Rate limiting between individual requests
                        await this.delay(500);
                        
                    } catch (error) {
                        console.error(`         ❌ Failed to fetch listing ${listingId}:`, error.message);
                        continue;
                    }
                }
                
                console.log(`         ✅ Batch ${Math.floor(i/batchSize) + 1}: ${detailedListings.length}/${listingIds.length} fetched`);
                
                // Longer delay between batches
                await this.delay(2000);
            }
            
            return detailedListings;
            
        } catch (error) {
            console.error('Failed to fetch detailed listing data:', error.message);
            return detailedListings;
        }
    }

    /**
     * NEW: Fetch single listing details from API
     */
    async fetchSingleListingDetails(listingId, neighborhood) {
        try {
            const rapidApiKey = process.env.RAPIDAPI_KEY;
            if (!rapidApiKey) return null;

            const response = await axios.get(`https://streeteasy1.p.rapidapi.com/rental/${listingId}`, {
                headers: {
                    'X-RapidAPI-Key': rapidApiKey,
                    'X-RapidAPI-Host': 'streeteasy1.p.rapidapi.com'
                },
                timeout: 15000
            });

            if (response.data && response.data.rental) {
                const rental = response.data.rental;
                return {
                    id: listingId,
                    address: rental.address || 'Unknown Address',
                    price: rental.price || 0,
                    bedrooms: rental.bedrooms || 0,
                    bathrooms: rental.bathrooms || 0,
                    sqft: rental.sqft || 0,
                    description: rental.description || '',
                    neighborhood: neighborhood,
                    amenities: rental.amenities || [],
                    url: rental.url || `https://streeteasy.com/rental/${listingId}`,
                    listedAt: rental.listedAt || new Date().toISOString(),
                    source: 'streeteasy_api'
                };
            }
            
            return null;
            
        } catch (error) {
            // Don't log every single error - too noisy
            return null;
        }
    }

    /**
     * NEW: Cache ALL listings (not just rent-stabilized)
     */
    async cacheAllListings(listings) {
        if (listings.length === 0) return;
        
        try {
            const cacheData = listings.map(listing => ({
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
            
            // Try to save to comprehensive_listing_cache first
            const { error: comprehensiveError } = await this.supabase
                .from('comprehensive_listing_cache')
                .upsert(cacheData, { onConflict: 'listing_id' });
            
            if (comprehensiveError) {
                // Fallback to listing_cache if comprehensive table doesn't exist
                const { error: fallbackError } = await this.supabase
                    .from('listing_cache')
                    .upsert(cacheData, { onConflict: 'listing_id' });
                
                if (fallbackError) {
                    console.error('Failed to cache listings:', fallbackError.message);
                }
            }
            
        } catch (error) {
            console.error('Cache operation failed:', error.message);
        }
    }

    /**
     * SIMPLIFIED: Load rent-stabilized buildings from database only
     */
    async loadRentStabilizedBuildingsSimple() {
        try {
            const { data, error } = await this.supabase
                .from('rent_stabilized_buildings')
                .select('*');
            
            if (error) throw error;
            
            if (!data || data.length === 0) {
                console.log('   ⚠️ No rent-stabilized buildings in database');
                console.log('   💡 Make sure to manually upload DHCR data to rent_stabilized_buildings table');
                return [];
            }
            
            return data;
            
        } catch (error) {
            console.error('Failed to load stabilized buildings:', error.message);
            return [];
        }
    }

    /**
     * Identify rent-stabilized listings using legal indicators
     */
    async identifyRentStabilizedListings(allListings, stabilizedBuildings) {
        const rentStabilizedListings = [];
        
        // Use environment variable for confidence threshold
        const confidenceThreshold = parseInt(process.env.RENT_STABILIZED_CONFIDENCE_THRESHOLD) || 70;
        
        for (const listing of allListings) {
            const analysis = this.analyzeRentStabilizationLegal(listing, stabilizedBuildings);
            
            if (analysis.confidence >= confidenceThreshold) {
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
     * Analyze rent stabilization using legal indicators
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
                
                if (points >= 85) {
                    return { confidence: Math.min(100, confidence), factors, method };
                }
            }
        }
        
        // Check if building is in DHCR database
        const matchingBuilding = this.findMatchingStabilizedBuilding(listing.address, stabilizedBuildings);
        if (matchingBuilding) {
            confidence += 60;
            factors.push(`Building found in DHCR database`);
            method = 'dhcr_registered';
        }
        
        // Check circumstantial evidence
        for (const [keyword, points] of Object.entries(this.RENT_STABILIZED_INDICATORS.circumstantial)) {
            if (description.includes(keyword)) {
                confidence += points * 0.5;
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
     * NEW: Analyze ALL rent-stabilized listings (save regardless of market position)
     */
    async analyzeAllRentStabilizedListings(rentStabilizedListings, allListings) {
        console.log(`   💰 Analyzing ${rentStabilizedListings.length} rent-stabilized listings (saving ALL)...\n`);
        
        const analyzedStabilized = [];
        
        for (const stabilizedListing of rentStabilizedListings) {
            console.log(`     📍 ${stabilizedListing.address}`);
            
            try {
                // Get market comparables
                const marketComparables = this.getMarketRateComparables(stabilizedListing, allListings);
                
                if (marketComparables.length < 5) {
                    console.log(`       ⚠️ Insufficient market comparables (${marketComparables.length})`);
                    // Still save it, but with limited analysis
                    const marketClassification = 'insufficient_data';
                    analyzedStabilized.push({
                        ...stabilizedListing,
                        estimatedMarketRent: stabilizedListing.price, // Use actual rent as fallback
                        undervaluationPercent: 0,
                        potentialSavings: 0,
                        marketClassification: marketClassification,
                        undervaluationMethod: 'insufficient_comparables',
                        undervaluationConfidence: 0,
                        comparablesUsed: marketComparables.length,
                        adjustments: []
                    });
                    continue;
                }
                
                // Run undervaluation analysis
                const undervaluationAnalysis = await this.analyzeUndervaluation(
                    stabilizedListing,
                    marketComparables
                );
                
                if (!undervaluationAnalysis.success) {
                    console.log(`       ❌ Undervaluation analysis failed`);
                    // Still save it with basic data
                    analyzedStabilized.push({
                        ...stabilizedListing,
                        estimatedMarketRent: stabilizedListing.price,
                        undervaluationPercent: 0,
                        potentialSavings: 0,
                        marketClassification: 'analysis_failed',
                        undervaluationMethod: 'failed',
                        undervaluationConfidence: 0,
                        comparablesUsed: marketComparables.length,
                        adjustments: []
                    });
                    continue;
                }
                
                const percentBelowMarket = undervaluationAnalysis.percentBelowMarket;
                console.log(`       📊 ${percentBelowMarket.toFixed(1)}% below market ($${undervaluationAnalysis.estimatedMarketRent.toLocaleString()})`);
                
                // Classify and save ALL rent-stabilized listings
                const marketClassification = this.classifyMarketPosition(percentBelowMarket);
                const savings = Math.max(0, undervaluationAnalysis.estimatedMarketRent - stabilizedListing.price);

                analyzedStabilized.push({
                    ...stabilizedListing,
                    estimatedMarketRent: undervaluationAnalysis.estimatedMarketRent,
                    undervaluationPercent: percentBelowMarket,
                    potentialSavings: savings,
                    marketClassification: marketClassification,
                    undervaluationMethod: undervaluationAnalysis.method,
                    undervaluationConfidence: undervaluationAnalysis.confidence,
                    comparablesUsed: undervaluationAnalysis.comparablesUsed,
                    adjustments: undervaluationAnalysis.adjustments || []
                });
                
                // Log based on classification
                if (marketClassification === 'undervalued') {
                    console.log(`       ✅ UNDERVALUED! Savings: $${savings.toLocaleString()}/month`);
                } else if (marketClassification === 'moderately_undervalued') {
                    console.log(`       📊 MODERATELY UNDERVALUED: $${savings.toLocaleString()}/month`);
                } else if (marketClassification === 'overvalued') {
                    console.log(`       📈 ABOVE MARKET: $${Math.abs(undervaluationAnalysis.estimatedMarketRent - stabilizedListing.price).toLocaleString()}/month premium`);
                } else {
                    console.log(`       📊 MARKET RATE: ${percentBelowMarket.toFixed(1)}% vs market`);
                }
                
            } catch (error) {
                console.error(`       ❌ Analysis failed: ${error.message}`);
                continue;
            }
            
            console.log('');
        }
        
        return analyzedStabilized;
    }

    /**
     * Classify market position
     */
    classifyMarketPosition(percentBelowMarket) {
        if (percentBelowMarket >= this.UNDERVALUATION_THRESHOLD) {
            return 'undervalued';
        } else if (percentBelowMarket >= this.MODERATE_UNDERVALUATION_THRESHOLD) {
            return 'moderately_undervalued';
        } else if (percentBelowMarket >= this.OVERVALUED_THRESHOLD) {
            return 'market_rate';
        } else {
            return 'overvalued';
        }
    }

    /**
     * Get market rate comparables (exclude rent-stabilized)
     */
    getMarketRateComparables(targetListing, allListings) {
        const targetBedrooms = targetListing.bedrooms || 0;
        const targetNeighborhood = targetListing.neighborhood;
        
        return allListings.filter(comp => {
            if (comp.id === targetListing.id) return false;
            if (this.appearsRentStabilized(comp)) return false;
            if (!comp.price || comp.price <= 0) return false;
            if (comp.bedrooms === null || comp.bedrooms === undefined) return false;
            
            const bedroomDiff = Math.abs((comp.bedrooms || 0) - targetBedrooms);
            if (bedroomDiff > 1) return false;
            
            if (targetNeighborhood && comp.neighborhood !== targetNeighborhood) {
                const adjacentNeighborhoods = this.getAdjacentNeighborhoods(targetNeighborhood);
                if (!adjacentNeighborhoods.includes(comp.neighborhood)) return false;
            }
            
            if (comp.price < 1500 || comp.price > 15000) return false;
            
            return true;
        });
    }

    /**
     * Quick check if listing appears rent-stabilized
     */
    appearsRentStabilized(listing) {
        const description = (listing.description || '').toLowerCase();
        const strongIndicators = [
            'rent stabilized', 'rent-stabilized', 'stabilized unit',
            'dhcr', 'legal rent', 'preferential rent'
        ];
        return strongIndicators.some(indicator => description.includes(indicator));
    }

    /**
     * Basic undervaluation analysis
     */
    async analyzeUndervaluation(targetListing, marketComparables) {
        try {
            // Simple median-based analysis for now
            const comparableRents = marketComparables.map(comp => comp.price).sort((a, b) => a - b);
            const medianMarketRent = this.calculateMedian(comparableRents);
            
            const percentBelowMarket = ((medianMarketRent - targetListing.price) / medianMarketRent) * 100;
            
            return {
                success: true,
                estimatedMarketRent: medianMarketRent,
                percentBelowMarket: percentBelowMarket,
                method: 'median_comparable',
                confidence: 75, // Basic confidence
                comparablesUsed: marketComparables.length,
                adjustments: []
            };
            
        } catch (error) {
            return { success: false, reason: error.message };
        }
    }

    /**
     * Calculate median
     */
    calculateMedian(arr) {
        const sorted = [...arr].sort((a, b) => a - b);
        const mid = Math.floor(sorted.length / 2);
        return sorted.length % 2 !== 0 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
    }

   /**
     * Find matching stabilized building
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
     * Get adjacent neighborhoods
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
     * Utility: Add delay for rate limiting
     */
    async delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    /**
     * NEW: Save ALL results (not just undervalued)
     */
    async saveAllResults(analyzedStabilized) {
        if (analyzedStabilized.length === 0) return;
        
        try {
            const resultsData = analyzedStabilized.map(listing => ({
                listing_id: listing.id,
                address: listing.address,
                neighborhood: listing.neighborhood,
                monthly_rent: listing.price,
                estimated_market_rent: listing.estimatedMarketRent,
                undervaluation_percent: listing.undervaluationPercent,
                potential_monthly_savings: listing.potentialSavings,
                market_classification: listing.marketClassification, // NEW field
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
    generateFinalReport(analyzedStabilized) {
        console.log('\n🎉 RENT-STABILIZED LISTINGS REPORT (ALL MARKET POSITIONS)');
        console.log('='.repeat(70));
        
        if (analyzedStabilized.length === 0) {
            console.log('❌ No rent-stabilized listings found');
            return;
        }
        
        // Group by market classification
        const grouped = analyzedStabilized.reduce((acc, listing) => {
            const classification = listing.marketClassification || 'unknown';
            if (!acc[classification]) acc[classification] = [];
            acc[classification].push(listing);
            return acc;
        }, {});
        
        console.log(`✅ Found ${analyzedStabilized.length} rent-stabilized listings!\n`);
        
        // Report by category
        Object.entries(grouped).forEach(([classification, listings]) => {
            const count = listings.length;
            const percentage = ((count / analyzedStabilized.length) * 100).toFixed(1);
            
            console.log(`📊 ${classification.toUpperCase().replace('_', ' ')}: ${count} listings (${percentage}%)`);
            
            if (classification === 'undervalued' || classification === 'moderately_undervalued') {
                const avgSavings = listings.reduce((sum, l) => sum + l.potentialSavings, 0) / count;
                console.log(`   💰 Average monthly savings: $${avgSavings.toLocaleString()}`);
                
                // Show top 3 deals in this category
                const topDeals = listings
                    .sort((a, b) => b.potentialSavings - a.potentialSavings)
                    .slice(0, 3);
                
                topDeals.forEach((listing, index) => {
                    console.log(`   ${index + 1}. ${listing.address} - $${listing.potentialSavings.toLocaleString()}/month savings`);
                });
            }
            console.log('');
        });
        
        // Overall statistics
        const totalSavings = analyzedStabilized
            .filter(l => l.potentialSavings > 0)
            .reduce((sum, l) => sum + l.potentialSavings, 0);
        
        const undervaluedCount = (grouped.undervalued?.length || 0) + (grouped.moderately_undervalued?.length || 0);
        
        console.log('📈 SUMMARY STATISTICS');
        console.log(`   • Total rent-stabilized listings: ${analyzedStabilized.length}`);
        console.log(`   • Undervalued opportunities: ${undervaluedCount} (${((undervaluedCount/analyzedStabilized.length)*100).toFixed(1)}%)`);
        console.log(`   • Total potential monthly savings: $${totalSavings.toLocaleString()}`);
        console.log(`   • Total potential annual savings: $${(totalSavings * 12).toLocaleString()}`);
        
        if (grouped.undervalued?.length > 0) {
            const bestDeal = grouped.undervalued.sort((a, b) => b.potentialSavings - a.potentialSavings)[0];
            console.log(`   • Best deal: ${bestDeal.address} (${bestDeal.undervaluationPercent.toFixed(1)}% below market)`);
        }
        
        console.log('\n💡 NEXT STEPS');
        console.log('   1. Check your Supabase "undervalued_rent_stabilized" table for all results');
        console.log('   2. Filter by market_classification for different strategies:');
        console.log('      - "undervalued": Priority targets (15%+ below market)');
        console.log('      - "moderately_undervalued": Good opportunities (5-15% below market)');
        console.log('      - "market_rate": Fair market deals');
        console.log('      - "overvalued": Above market (may have other benefits)');
        console.log('   3. Verify rent stabilization status with landlord/DHCR before applying');
        console.log('   4. Contact listings immediately - good deals move fast in NYC');
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
            maxListingsPerNeighborhood: 2000, // Use environment variable
            testMode: false
        });
        
        console.log('\n🎉 Analysis complete!');
        console.log(`📊 Check your Supabase 'undervalued_rent_stabilized' table for ${results.allRentStabilizedSaved} listings`);
        console.log(`🎯 All rent-stabilized listings saved regardless of market position`);
        
        return results;
        
    } catch (error) {
        console.error('💥 Analysis failed:', error);
        process.exit(1);
    }
}

// Run if executed directly
if (require.main === module) {
    main().catch(error => {
        console.error('💥 Rent-stabilized detector crashed:', error);
        process.exit(1);
    });
}
