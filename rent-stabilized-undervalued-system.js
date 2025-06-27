/**
 * NYC RENT-STABILIZED APARTMENT FINDER - FINAL CORRECTED VERSION
 * 
 * CORRECTED CACHING FLOW:
 * 1. Basic Search → Get listing IDs only (77 IDs for SoHo)
 * 2. Check Cache → See which IDs we already have with full details  
 * 3. Individual Fetches → Only fetch details for new IDs we don't have
 * 4. Cache After Details → Save complete data with addresses
 * 5. Analysis → Use complete cached data for rent-stabilized analysis
 * 
 * SAVES ONLY TO: undervalued_rent_stabilized table (NO undervalued_rentals)
 * NO RENT-STABILIZED BUILDING LIMITS: Finds ALL rent-stabilized properties
 */

const axios = require('axios');
const { createClient } = require('@supabase/supabase-js');
require('dotenv').config();

class RentStabilizedDetector {
    constructor() {
        this.supabase = createClient(
            process.env.SUPABASE_URL,
            process.env.SUPABASE_ANON_KEY
        );
        
        // Default high-priority neighborhoods
        this.defaultNeighborhoods = [
            'east-village', 'lower-east-side', 'chinatown', 'financial-district',
            'west-village', 'greenwich-village', 'soho', 'nolita', 'tribeca',
            'chelsea', 'gramercy', 'murray-hill', 'kips-bay', 'flatiron',
            'upper-east-side', 'upper-west-side', 'hells-kitchen', 'midtown-east',
            'williamsburg', 'dumbo', 'brooklyn-heights', 'cobble-hill',
            'carroll-gardens', 'park-slope', 'fort-greene', 'boerum-hill',
            'red-hook', 'prospect-heights', 'crown-heights', 'bedford-stuyvesant',
            'greenpoint', 'bushwick', 'long-island-city', 'astoria', 'sunnyside'
        ];
        
        // Rent stabilization indicators (legal analysis)
        this.RENT_STABILIZED_INDICATORS = {
            explicit: {
                'rent stabilized': 100,
                'rent-stabilized': 100,
                'stabilized unit': 95,
                'dhcr registered': 90,
                'legal rent': 85,
                'preferential rent': 90,
                'regulated apartment': 85
            },
            circumstantial: {
                'prewar': 45,
                'pre-war': 45, 
                'walk-up': 40,
                'walkup': 40,
                'established tenant': 30
            }
        };
        
        // Undervaluation method types (from SQL schema)
        this.UNDERVALUATION_METHODS = {
            EXACT_MATCH: 'exact_bed_bath_amenity_match',
            BED_BATH_SPECIFIC: 'bed_bath_specific_pricing', 
            BED_SPECIFIC: 'bed_specific_with_adjustments',
            PRICE_PER_SQFT: 'price_per_sqft_fallback'
        };

        this.apiUsageStats = {
            totalCalls: 0,
            successfulCalls: 0,
            failedCalls: 0,
            cacheHits: 0
        };
    }

    /**
     * MAIN FUNCTION: Find rent-stabilized listings (matches railway-sequential-runner.js expectations)
     */
    async findUndervaluedRentStabilizedListings(options = {}) {
        try {
            console.log('🏙️ NYC RENT-STABILIZED APARTMENT FINDER');
            console.log('=' .repeat(60));
            console.log('🎯 GOAL: Find ALL rent-stabilized apartments using CORRECTED caching flow');
            console.log('💾 SAVE TO: undervalued_rent_stabilized table ONLY\n');

            const {
                neighborhoods = this.defaultNeighborhoods.slice(0, 5), // Start with top 5
                maxListingsPerNeighborhood = 500,
                testMode = false
            } = options;

            // STEP 1: Get ALL listings with CORRECTED caching flow
            console.log('📋 Step 1: Fetching listings with CORRECTED caching flow...');
            const allListings = await this.getAllListingsWithCorrectedCaching(neighborhoods, maxListingsPerNeighborhood);
            console.log(`   ✅ Total listings collected: ${allListings.length}\n`);

            if (allListings.length === 0) {
                console.log('❌ No listings found. Check API key and neighborhood names.');
                return { totalListingsScanned: 0, rentStabilizedFound: 0, allRentStabilizedSaved: 0, results: [] };
            }

            // STEP 2: Load rent-stabilized buildings from database
            console.log('🏢 Step 2: Loading rent-stabilized buildings database...');
            const stabilizedBuildings = await this.loadRentStabilizedBuildings();
            console.log(`   ✅ Loaded ${stabilizedBuildings.length} stabilized buildings\n`);

            // STEP 3: Identify rent-stabilized listings using legal indicators
            console.log('⚖️ Step 3: Identifying rent-stabilized listings...');
            const rentStabilizedListings = await this.identifyRentStabilizedListings(allListings, stabilizedBuildings);
            console.log(`   ✅ Found ${rentStabilizedListings.length} rent-stabilized listings\n`);

            // STEP 4: Analyze ALL rent-stabilized listings with market classification
            console.log('💰 Step 4: Analyzing market position for ALL rent-stabilized listings...');
            const analyzedListings = await this.analyzeAllRentStabilizedListings(rentStabilizedListings, allListings);
            console.log(`   ✅ Analyzed ${analyzedListings.length} rent-stabilized listings\n`);

            // STEP 5: Save ALL results to undervalued_rent_stabilized table
            console.log('💾 Step 5: Saving ALL results to undervalued_rent_stabilized table...');
            await this.saveAllRentStabilizedResults(analyzedListings);
            console.log(`   ✅ Saved ${analyzedListings.length} rent-stabilized listings\n`);

            // Generate final report
            this.generateFinalReport(analyzedListings);

            return {
                totalListingsScanned: allListings.length,
                rentStabilizedFound: rentStabilizedListings.length,
                allRentStabilizedSaved: analyzedListings.length,
                results: analyzedListings.sort((a, b) => b.undervaluation_percent - a.undervaluation_percent)
            };

        } catch (error) {
            console.error('💥 Analysis failed:', error.message);
            throw error;
        }
    }

    /**
     * CORRECTED CACHING FLOW: Get all listings with proper caching
     */
    async getAllListingsWithCorrectedCaching(neighborhoods, maxPerNeighborhood) {
        const allListings = [];
        
        for (const neighborhood of neighborhoods) {
            console.log(`   📍 Processing ${neighborhood} with CORRECTED caching flow...`);
            
            try {
                // STEP 1: Get existing cached listings with FULL DETAILS
                const cachedListings = await this.getCachedListingsWithFullDetails(neighborhood);
                console.log(`     💾 Cache: ${cachedListings.length} listings with full details`);
                
                // STEP 2: Get basic listing IDs from StreetEasy (NO ADDRESSES YET)
                const basicListingIds = await this.fetchBasicListingIds(neighborhood, maxPerNeighborhood);
                console.log(`     🔍 Search: ${basicListingIds.length} listing IDs found`);
                
                // STEP 3: Find which listing IDs need individual fetching
                const cachedIds = new Set(cachedListings.map(l => l.id));
                const newIds = basicListingIds.filter(item => !cachedIds.has(item.id));
                console.log(`     ✨ New: ${newIds.length} listings need individual fetching`);
                
                // STEP 4: Fetch individual details for NEW listings only
                const newListingsWithDetails = [];
                if (newIds.length > 0) {
                    console.log(`     🔄 Fetching individual details...`);
                    
                    for (const basicItem of newIds) {
                        try {
                            const detailListing = await this.fetchIndividualListingDetails(basicItem.id);
                            if (detailListing && detailListing.address && detailListing.address !== 'Unknown Address') {
                                newListingsWithDetails.push(detailListing);
                                console.log(`       ✅ ${detailListing.address.substring(0, 40)}...`);
                            } else {
                                console.log(`       ❌ No address for ${basicItem.id}`);
                            }
                            
                            // Rate limiting
                            await this.delay(1200);
                        } catch (error) {
                            console.log(`       ❌ Error fetching ${basicItem.id}: ${error.message}`);
                        }
                    }
                }
                
                // STEP 5: Cache the new listings with full details
                if (newListingsWithDetails.length > 0) {
                    await this.cacheListingsWithFullDetails(newListingsWithDetails);
                    console.log(`     ✅ Cached ${newListingsWithDetails.length} new listings`);
                }
                
                // STEP 6: Combine cached + new listings
                const combinedListings = [...cachedListings, ...newListingsWithDetails];
                allListings.push(...combinedListings);
                
                const efficiency = basicListingIds.length > 0 ? 
                    Math.round((cachedListings.length / basicListingIds.length) * 100) : 100;
                console.log(`     📊 Total: ${combinedListings.length} listings (${efficiency}% cache efficiency)\n`);
                
            } catch (error) {
                console.error(`     ❌ Error processing ${neighborhood}:`, error.message);
            }
        }
        
        return allListings;
    }

    /**
     * Get cached listings with full details ONLY
     */
    async getCachedListingsWithFullDetails(neighborhood) {
        try {
            const { data, error } = await this.supabase
                .from('comprehensive_listing_cache')
                .select('*')
                .eq('neighborhood', neighborhood)
                .not('address', 'is', null)
                .neq('address', 'Unknown Address')
                .neq('address', '');
            
            if (error || !data) {
                return [];
            }
            
            // Convert to standard format
            return data.map(row => ({
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
                source: 'cache_with_full_details'
            }));
            
        } catch (error) {
            console.error('Failed to get cached listings:', error.message);
            return [];
        }
    }

    /**
     * Fetch basic listing IDs only (no addresses)
     */
    async fetchBasicListingIds(neighborhood, maxListings) {
        try {
            const rapidApiKey = process.env.RAPIDAPI_KEY;
            if (!rapidApiKey) {
                console.log(`       ⚠️ No RAPIDAPI_KEY found`);
                return [];
            }

            this.apiUsageStats.totalCalls++;
            
            const response = await axios.get('https://streeteasy-api.p.rapidapi.com/rentals/search', {
                headers: {
                    'X-RapidAPI-Key': rapidApiKey,
                    'X-RapidAPI-Host': 'streeteasy-api.p.rapidapi.com'
                },
                params: {
                    areas: neighborhood,
                    limit: Math.min(maxListings, 500),
                    minPrice: 1000,
                    maxPrice: 20000
                },
                timeout: 30000
            });

            this.apiUsageStats.successfulCalls++;

            // Extract IDs from various possible response structures
            let listings = [];
            if (response.data) {
                if (response.data.results) listings = response.data.results;
                else if (response.data.listings) listings = response.data.listings;
                else if (response.data.rentals) listings = response.data.rentals;
                else if (Array.isArray(response.data)) listings = response.data;
            }

            // Return just IDs and basic info
            const basicIds = listings
                .map(item => ({ id: item.id?.toString(), neighborhood }))
                .filter(item => item.id);

            console.log(`       🔍 API returned ${basicIds.length} listing IDs`);
            return basicIds;

        } catch (error) {
            this.apiUsageStats.failedCalls++;
            console.error(`       ❌ Basic search failed for ${neighborhood}:`, error.message);
            return [];
        }
    }

    /**
     * Fetch individual listing details with full address
     */
    async fetchIndividualListingDetails(listingId) {
        try {
            const rapidApiKey = process.env.RAPIDAPI_KEY;
            if (!rapidApiKey) return null;

            this.apiUsageStats.totalCalls++;

            const response = await axios.get(`https://streeteasy-api.p.rapidapi.com/rentals/${listingId}`, {
                headers: {
                    'X-RapidAPI-Key': rapidApiKey,
                    'X-RapidAPI-Host': 'streeteasy-api.p.rapidapi.com'
                },
                timeout: 30000
            });

            this.apiUsageStats.successfulCalls++;

            if (response.data) {
                const data = response.data;
                return {
                    id: listingId,
                    address: data.address || data.full_address || data.street_address || 'Unknown Address',
                    price: data.price || data.monthly_rent || data.rent || 0,
                    bedrooms: data.bedrooms || 0,
                    bathrooms: data.bathrooms || 0,
                    sqft: data.sqft || data.square_feet || 0,
                    description: data.description || data.details || '',
                    neighborhood: data.neighborhood || data.area || '',
                    amenities: data.amenities || [],
                    url: data.url || `https://streeteasy.com/rental/${listingId}`,
                    listedAt: data.listedAt || data.listed_at || new Date().toISOString(),
                    source: 'individual_fetch'
                };
            }

            return null;

        } catch (error) {
            this.apiUsageStats.failedCalls++;
            console.error(`Failed to fetch individual details for ${listingId}:`, error.message);
            return null;
        }
    }

    /**
     * Cache listings with full details
     */
    async cacheListingsWithFullDetails(listings) {
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
            
            const { error } = await this.supabase
                .from('comprehensive_listing_cache')
                .upsert(cacheData, { onConflict: 'listing_id' });
            
            if (error) {
                console.error('Failed to cache listings:', error.message);
            }
            
        } catch (error) {
            console.error('Cache operation failed:', error.message);
        }
    }

    /**
     * Rate limiting delay
     */
    async delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    /**
     * Load rent-stabilized buildings from database
     */
    async loadRentStabilizedBuildings() {
        try {
            const { data, error } = await this.supabase
                .from('rent_stabilized_buildings')
                .select('*');
            
            if (error) {
                console.log(`   ⚠️ Database error: ${error.message}`);
                return [];
            }
            
            if (!data || data.length === 0) {
                console.log('   ⚠️ No rent-stabilized buildings in database');
                console.log('   💡 System will use rent-stabilization indicators instead');
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
        const confidenceThreshold = parseInt(process.env.RENT_STABILIZED_CONFIDENCE_THRESHOLD) || 40;
        
        console.log(`   🎯 Using confidence threshold: ${confidenceThreshold}%`);
        
        for (const listing of allListings) {
            const analysis = this.analyzeRentStabilization(listing, stabilizedBuildings);
            
            if (analysis.confidence >= confidenceThreshold) {
                rentStabilizedListings.push({
                    ...listing,
                    rentStabilizedConfidence: analysis.confidence,
                    rentStabilizedMethod: analysis.method,
                    rentStabilizedFactors: analysis.factors
                });
                
                console.log(`     ✅ ${listing.address.substring(0, 40)}... (${analysis.confidence}%)`);
            }
        }
        
        return rentStabilizedListings;
    }

    /**
     * Analyze rent stabilization using legal indicators
     */
    analyzeRentStabilization(listing, stabilizedBuildings) {
        let confidence = 0;
        const factors = [];
        let method = 'circumstantial';

        // Check for DHCR building match (strongest indicator)
        const matchedBuilding = this.findMatchingStabilizedBuilding(listing.address, stabilizedBuildings);
        if (matchedBuilding) {
            confidence += 60;
            factors.push('DHCR registered building');
            method = 'dhcr_registered';
        }

        // Check for explicit mentions in description
        const description = (listing.description || '').toLowerCase();
        for (const [keyword, points] of Object.entries(this.RENT_STABILIZED_INDICATORS.explicit)) {
            if (description.includes(keyword)) {
                confidence += points;
                factors.push(`Explicit mention: "${keyword}"`);
                if (method === 'circumstantial') method = 'explicit_mention';
            }
        }

        // Check for circumstantial evidence
        for (const [keyword, points] of Object.entries(this.RENT_STABILIZED_INDICATORS.circumstantial)) {
            if (description.includes(keyword)) {
                confidence += points * 0.5; // Reduced weight
                factors.push(`Circumstantial: "${keyword}"`);
            }
        }

        // Building age analysis (pre-1974 buildings are more likely stabilized)
        const yearBuilt = this.extractYearBuilt(description);
        if (yearBuilt && yearBuilt < 1974) {
            confidence += 25;
            factors.push(`Built before 1974 (${yearBuilt})`);
            if (method === 'circumstantial') method = 'building_analysis';
        }

        return {
            confidence: Math.min(100, Math.round(confidence)),
            factors,
            method
        };
    }

    /**
     * Find matching stabilized building by address
     */
    findMatchingStabilizedBuilding(address, stabilizedBuildings) {
        if (!address || stabilizedBuildings.length === 0) return null;
        
        const normalizedAddress = this.normalizeAddress(address);
        
        return stabilizedBuildings.find(building => {
            const buildingAddress = this.normalizeAddress(building.address || '');
            return buildingAddress && normalizedAddress.includes(buildingAddress.substring(0, 15));
        });
    }

    /**
     * Normalize address for comparison
     */
    normalizeAddress(address) {
        return address
            .toLowerCase()
            .replace(/[^\w\s]/g, ' ')
            .replace(/\s+/g, ' ')
            .trim();
    }

    /**
     * Extract year built from description
     */
    extractYearBuilt(description) {
        const yearMatch = description.match(/built.{0,10}(19\d{2}|20\d{2})/i);
        return yearMatch ? parseInt(yearMatch[1]) : null;
    }

    /**
     * Analyze ALL rent-stabilized listings with market classification
     */
    async analyzeAllRentStabilizedListings(rentStabilizedListings, allListings) {
        const analyzedListings = [];
        
        for (const listing of rentStabilizedListings) {
            try {
                // Get market rate comparables (exclude other rent-stabilized units)
                const comparables = this.getMarketRateComparables(listing, allListings);
                
                if (comparables.length < 5) {
                    // Insufficient data - still save with basic info
                    analyzedListings.push({
                        ...listing,
                        estimated_market_rent: listing.price,
                        undervaluation_percent: 0,
                        potential_monthly_savings: 0,
                        market_classification: 'insufficient_data',
                        undervaluation_method: 'insufficient_comparables',
                        undervaluation_confidence: 0,
                        comparables_used: comparables.length
                    });
                    continue;
                }

                // Perform market analysis
                const marketAnalysis = this.performMarketAnalysis(listing, comparables);
                
                // Classify the listing
                const classification = this.classifyListing(marketAnalysis.undervaluationPercent);
                
                analyzedListings.push({
                    ...listing,
                    estimated_market_rent: marketAnalysis.estimatedMarketRent,
                    undervaluation_percent: marketAnalysis.undervaluationPercent,
                    potential_monthly_savings: marketAnalysis.potentialSavings,
                    market_classification: classification,
                    undervaluation_method: marketAnalysis.method,
                    undervaluation_confidence: marketAnalysis.confidence,
                    comparables_used: comparables.length
                });

            } catch (error) {
                console.error(`Failed to analyze listing ${listing.id}:`, error.message);
            }
        }
        
        return analyzedListings;
    }

    /**
     * Get market rate comparables (exclude rent-stabilized units)
     */
    getMarketRateComparables(targetListing, allListings) {
        return allListings.filter(comp => {
            // Skip the target listing
            if (comp.id === targetListing.id) return false;
            
            // Skip likely rent-stabilized listings
            if (this.appearsRentStabilized(comp)) return false;
            
            // Basic data quality
            if (!comp.price || comp.price <= 0) return false;
            
            // Same neighborhood and similar bedroom count
            if (comp.neighborhood !== targetListing.neighborhood) return false;
            if (Math.abs((comp.bedrooms || 0) - (targetListing.bedrooms || 0)) > 1) return false;
            
            // Price sanity check
            if (comp.price < 1500 || comp.price > 15000) return false;
            
            return true;
        });
    }

    /**
     * Quick check if listing appears rent-stabilized
     */
    appearsRentStabilized(listing) {
        const description = (listing.description || '').toLowerCase();
        const indicators = ['rent stabilized', 'rent-stabilized', 'dhcr', 'legal rent'];
        return indicators.some(indicator => description.includes(indicator));
    }

    /**
     * ADVANCED: Perform sophisticated market analysis using full valuation system
     */
    performMarketAnalysis(listing, comparables) {
        try {
            // Run sophisticated undervaluation analysis
            const undervaluationAnalysis = this.analyzeUndervaluation(listing, comparables);
            
            if (!undervaluationAnalysis.success) {
                // Fallback to basic analysis
                return this.performBasicMarketAnalysis(listing, comparables);
            }
            
            return {
                estimatedMarketRent: undervaluationAnalysis.estimatedMarketRent,
                undervaluationPercent: undervaluationAnalysis.percentBelowMarket,
                potentialSavings: Math.max(0, undervaluationAnalysis.estimatedMarketRent - listing.price),
                method: undervaluationAnalysis.method,
                confidence: undervaluationAnalysis.confidence
            };
            
        } catch (error) {
            console.error(`Advanced analysis failed for ${listing.id}:`, error.message);
            return this.performBasicMarketAnalysis(listing, comparables);
        }
    }

    /**
     * BASIC: Fallback market analysis
     */
    performBasicMarketAnalysis(listing, comparables) {
        // Use median pricing for stability
        const comparableRents = comparables.map(c => c.price).sort((a, b) => a - b);
        const medianMarketRent = this.calculateMedian(comparableRents);
        
        // Calculate undervaluation
        const undervaluationPercent = ((medianMarketRent - listing.price) / medianMarketRent) * 100;
        const potentialSavings = Math.max(0, medianMarketRent - listing.price);
        
        // Determine method based on match quality
        let method = this.UNDERVALUATION_METHODS.PRICE_PER_SQFT;
        let confidence = 60;
        
        const exactMatches = comparables.filter(c => 
            c.bedrooms === listing.bedrooms && 
            Math.abs((c.bathrooms || 0) - (listing.bathrooms || 0)) <= 0.5
        );
        
        if (exactMatches.length >= 3) {
            method = this.UNDERVALUATION_METHODS.EXACT_MATCH;
            confidence = 85;
        } else if (comparables.length >= 8) {
            method = this.UNDERVALUATION_METHODS.BED_BATH_SPECIFIC;
            confidence = 75;
        }
        
        return {
            estimatedMarketRent: Math.round(medianMarketRent),
            undervaluationPercent: Math.round(undervaluationPercent * 100) / 100,
            potentialSavings: Math.round(potentialSavings),
            method,
            confidence
        };
    }

    /**
     * ADVANCED: Sophisticated undervaluation analysis using full system
     */
    analyzeUndervaluation(targetListing, marketComparables) {
        try {
            // STEP 1: Advanced valuation method selection
            const valuationResult = this.selectAdvancedValuationMethod(targetListing, marketComparables);
            
            if (!valuationResult.success) {
                return { success: false, reason: valuationResult.reason };
            }
            
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
                totalAdjustments: adjustedMarketValue.totalAdjustments
            };
            
        } catch (error) {
            return { success: false, reason: error.message };
        }
    }

    /**
     * ADVANCED: Select best valuation method
     */
    selectAdvancedValuationMethod(targetProperty, comparables) {
        const beds = targetProperty.bedrooms || 0;
        const baths = targetProperty.bathrooms || 0;
        
        // METHOD 1: Exact bed/bath/amenity match (MOST ACCURATE)
        const exactMatches = comparables.filter(comp => 
            comp.bedrooms === beds && 
            Math.abs((comp.bathrooms || 0) - baths) <= 0.5 &&
            this.hasReasonableDataQuality(comp)
        );
        
        if (exactMatches.length >= 3) {
            return {
                success: true,
                method: 'exact_bed_bath_amenity_match',
                comparables: exactMatches
            };
        }

        // METHOD 2: Bed/bath specific pricing
        const bedBathMatches = comparables.filter(comp => 
            comp.bedrooms === beds && 
            this.hasReasonableDataQuality(comp)
        );
        
        if (bedBathMatches.length >= 8) {
            return {
                success: true,
                method: 'bed_bath_specific_pricing',
                comparables: bedBathMatches
            };
        }

        // METHOD 3: Bedroom specific with bathroom adjustments
        const bedroomMatches = comparables.filter(comp => 
            comp.bedrooms === beds && 
            this.hasReasonableDataQuality(comp)
        );
        
        if (bedroomMatches.length >= 12) {
            return {
                success: true,
                method: 'bed_specific_with_adjustments',
                comparables: bedroomMatches
            };
        }

        // METHOD 4: Price per sqft fallback (LAST RESORT)
        const sqftComparables = comparables.filter(comp => 
            (comp.sqft || 0) > 0 && (comp.price || 0) > 0 &&
            this.hasReasonableDataQuality(comp)
        );
        
        if (sqftComparables.length >= 20) {
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
     * Method 3: Bedroom specific with bathroom adjustments
     */
    calculateBedroomBasedValueWithBathAdjustments(targetProperty, comparables) {
        const targetBaths = targetProperty.bathrooms || 0;
        
        // Calculate price-per-bedroom baseline
        const adjustedRents = comparables.map(comp => {
            const bathDiff = targetBaths - (comp.bathrooms || 0);
            const bathAdjustment = this.calculateBathroomAdjustment(bathDiff);
            return comp.price + bathAdjustment;
        }).filter(rent => rent > 0).sort((a, b) => a - b);
        
        const median = this.calculateMedian(adjustedRents);
        
        return {
            baseValue: median,
            method: 'bedroom_with_bath_adjustments',
            dataPoints: adjustedRents.length,
            rentRange: { min: Math.min(...adjustedRents), max: Math.max(...adjustedRents) }
        };
    }

    /**
     * Method 4: Square footage based pricing (fallback)
     */
    calculateSqftBasedValue(targetProperty, comparables) {
        const targetSqft = targetProperty.sqft || this.estimateSquareFootage(targetProperty.bedrooms || 0);
        
        // Calculate rent per sqft from comparables
        const rentPerSqftValues = comparables
            .map(comp => {
                const sqft = comp.sqft || this.estimateSquareFootage(comp.bedrooms || 0);
                return comp.price / sqft;
            })
            .filter(value => value > 0)
            .sort((a, b) => a - b);
        
        const medianRentPerSqft = this.calculateMedian(rentPerSqftValues);
        const estimatedRent = medianRentPerSqft * targetSqft;
        
        return {
            baseValue: estimatedRent,
            method: 'price_per_sqft',
            dataPoints: rentPerSqftValues.length,
            rentPerSqft: medianRentPerSqft,
            estimatedSqft: targetSqft
        };
    }

    /**
     * Apply sophisticated market adjustments
     */
    applyAdvancedMarketAdjustments(targetProperty, baseValue, comparables, method) {
        const adjustments = [];
        let totalAdjustment = 0;
        const borough = this.getBoroughFromNeighborhood(targetProperty.neighborhood).toLowerCase();
        
        // STEP 1: Amenity adjustments (sophisticated)
        const targetAmenities = this.extractAmenitiesFromListing(targetProperty);
        const amenityAdjustment = this.calculateAdvancedAmenityAdjustments(targetAmenities, baseValue.baseValue, borough);
        
        if (Math.abs(amenityAdjustment) > 25) { // Only apply significant adjustments
            adjustments.push({
                type: 'amenities',
                amount: amenityAdjustment,
                description: `Amenity adjustments based on ${targetAmenities.length} features`
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

        const finalValue = Math.round(baseValue.baseValue + totalAdjustment);
        
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
            'roof deck': 100,
            'terrace': 150,
            'balcony': 125,
            'parking': 250,
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
        
        // Calculate average sqft of comparables
        const validComparables = comparables.filter(comp => comp.sqft && comp.sqft > 0);
        if (validComparables.length === 0) return 0;
        
        const avgSqft = validComparables.reduce((sum, comp) => sum + comp.sqft, 0) / validComparables.length;
        const sqftDifference = targetSqft - avgSqft;
        
        // NYC rent per sqft premium/discount (varies by borough)
        const rentPerSqftAdjustment = 3; // $3 per sqft difference
        
        return Math.round(sqftDifference * rentPerSqftAdjustment);
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
     * Calculate median value
     */
    calculateMedian(values) {
        if (values.length === 0) return 0;
        const sorted = [...values].sort((a, b) => a - b);
        const mid = Math.floor(sorted.length / 2);
        return sorted.length % 2 === 0 ? 
            (sorted[mid - 1] + sorted[mid]) / 2 : 
            sorted[mid];
    }

    /**
     * Classify listing based on undervaluation percentage
     */
    classifyListing(undervaluationPercent) {
        if (undervaluationPercent >= 15) return 'undervalued';
        if (undervaluationPercent >= 5) return 'moderately_undervalued';
        if (undervaluationPercent >= -5) return 'market_rate';
        return 'overvalued';
    }

    /**
     * Save ALL rent-stabilized results to undervalued_rent_stabilized table
     */
    async saveAllRentStabilizedResults(analyzedListings) {
        if (analyzedListings.length === 0) {
            console.log('   ⚠️ No rent-stabilized listings to save');
            return;
        }
        
        try {
            // Map to exact undervalued_rent_stabilized table structure from SQL schema
            const saveData = analyzedListings.map(listing => ({
                // Required fields
                listing_id: listing.id,
                listing_url: listing.url,
                address: listing.address,
                neighborhood: listing.neighborhood,
                monthly_rent: listing.price,
                estimated_market_rent: listing.estimated_market_rent,
                undervaluation_percent: listing.undervaluation_percent,
                potential_monthly_savings: listing.potential_monthly_savings,
                
                // Property details
                bedrooms: listing.bedrooms || null,
                bathrooms: listing.bathrooms || null,
                sqft: listing.sqft || null,
                description: listing.description || null,
                amenities: listing.amenities || [],
                
                // Rent stabilization analysis
                rent_stabilized_confidence: listing.rentStabilizedConfidence,
                rent_stabilized_method: listing.rentStabilizedMethod,
                rent_stabilization_analysis: {
                    explanation: `${listing.rentStabilizedFactors.join(', ')}`,
                    key_factors: listing.rentStabilizedFactors || [],
                    probability: listing.rentStabilizedConfidence,
                    confidence_breakdown: {}
                },
                
                // Undervaluation analysis
                undervaluation_method: listing.undervaluation_method,
                undervaluation_confidence: listing.undervaluation_confidence,
                comparables_used: listing.comparables_used,
                undervaluation_analysis: {
                    methodology: listing.undervaluation_method,
                    base_market_rent: listing.estimated_market_rent,
                    final_market_estimate: listing.estimated_market_rent,
                    comparable_properties: []
                },
                
                // Borough (derived from neighborhood)
                borough: this.getBoroughFromNeighborhood(listing.neighborhood),
                
                // Market classification (NEW - based on undervaluation)
                market_classification: listing.market_classification,
                
                // Timestamps
                discovered_at: new Date().toISOString(),
                analyzed_at: new Date().toISOString(),
                last_verified: new Date().toISOString()
            }));
            
            // Save to undervalued_rent_stabilized table ONLY
            const { data, error } = await this.supabase
                .from('undervalued_rent_stabilized')
                .upsert(saveData, { 
                    onConflict: 'listing_id',
                    ignoreDuplicates: false 
                });
            
            if (error) {
                console.error('❌ Failed to save to undervalued_rent_stabilized:', error.message);
                console.log('💡 Check that all required columns exist in your table');
                throw error;
            }
            
            console.log(`   ✅ Successfully saved ${saveData.length} rent-stabilized listings`);
            
        } catch (error) {
            console.error('Save operation failed:', error.message);
            throw error;
        }
    }

    /**
     * Get borough from neighborhood
     */
    getBoroughFromNeighborhood(neighborhood) {
        const manhattanNeighborhoods = [
            'east-village', 'lower-east-side', 'chinatown', 'financial-district',
            'west-village', 'greenwich-village', 'soho', 'nolita', 'tribeca',
            'chelsea', 'gramercy', 'murray-hill', 'kips-bay', 'flatiron',
            'upper-east-side', 'upper-west-side', 'hells-kitchen', 'midtown-east'
        ];
        
        const brooklynNeighborhoods = [
            'williamsburg', 'dumbo', 'brooklyn-heights', 'cobble-hill',
            'carroll-gardens', 'park-slope', 'fort-greene', 'boerum-hill',
            'red-hook', 'prospect-heights', 'crown-heights', 'bedford-stuyvesant',
            'greenpoint', 'bushwick'
        ];
        
        const queensNeighborhoods = [
            'long-island-city', 'astoria', 'sunnyside', 'woodside',
            'jackson-heights', 'elmhurst', 'forest-hills', 'ridgewood'
        ];
        
        if (manhattanNeighborhoods.includes(neighborhood)) return 'Manhattan';
        if (brooklynNeighborhoods.includes(neighborhood)) return 'Brooklyn';
        if (queensNeighborhoods.includes(neighborhood)) return 'Queens';
        return 'Unknown';
    }

    /**
     * Generate comprehensive final report
     */
    generateFinalReport(analyzedListings) {
        console.log('🎉 RENT-STABILIZED APARTMENT ANALYSIS COMPLETE!');
        console.log('=' .repeat(60));
        
        if (analyzedListings.length === 0) {
            console.log('❌ No rent-stabilized listings found');
            return;
        }
        
        // Classification breakdown
        const undervalued = analyzedListings.filter(l => l.market_classification === 'undervalued');
        const moderatelyUndervalued = analyzedListings.filter(l => l.market_classification === 'moderately_undervalued');
        const marketRate = analyzedListings.filter(l => l.market_classification === 'market_rate');
        const overvalued = analyzedListings.filter(l => l.market_classification === 'overvalued');
        const insufficientData = analyzedListings.filter(l => l.market_classification === 'insufficient_data');
        
        console.log(`🏠 TOTAL RENT-STABILIZED APARTMENTS: ${analyzedListings.length}`);
        console.log(`💰 Undervalued (15%+ below market): ${undervalued.length}`);
        console.log(`📊 Moderately undervalued (5-15% below): ${moderatelyUndervalued.length}`);
        console.log(`📈 Market rate (±5%): ${marketRate.length}`);
        console.log(`⚠️ Above market (5%+ above): ${overvalued.length}`);
        console.log(`❓ Insufficient data: ${insufficientData.length}`);
        
        // Top opportunities
        const topOpportunities = [...undervalued, ...moderatelyUndervalued]
            .sort((a, b) => b.undervaluation_percent - a.undervaluation_percent)
            .slice(0, 5);
        
        if (topOpportunities.length > 0) {
            console.log('\n🌟 TOP RENT-STABILIZED OPPORTUNITIES:');
            topOpportunities.forEach((listing, index) => {
                console.log(`   ${index + 1}. ${listing.address}`);
                console.log(`      💰 ${listing.monthly_rent.toLocaleString()}/month (${listing.undervaluation_percent}% below market)`);
                console.log(`      💎 Monthly savings: ${listing.potential_monthly_savings.toLocaleString()}`);
                console.log(`      🏠 ${listing.bedrooms}BR/${listing.bathrooms}BA, ${listing.sqft || 'Unknown'} sqft`);
                console.log(`      ⚖️ Rent-stabilized confidence: ${listing.rentStabilizedConfidence}%\n`);
            });
        }
        
        // Neighborhood breakdown
        const neighborhoodStats = {};
        analyzedListings.forEach(listing => {
            const neighborhood = listing.neighborhood;
            if (!neighborhoodStats[neighborhood]) {
                neighborhoodStats[neighborhood] = { total: 0, undervalued: 0 };
            }
            neighborhoodStats[neighborhood].total++;
            if (listing.market_classification === 'undervalued' || listing.market_classification === 'moderately_undervalued') {
                neighborhoodStats[neighborhood].undervalued++;
            }
        });
        
        console.log('📍 NEIGHBORHOOD BREAKDOWN:');
        Object.entries(neighborhoodStats)
            .sort(([,a], [,b]) => b.total - a.total)
            .slice(0, 10)
            .forEach(([neighborhood, stats]) => {
                const undervaluedPercent = Math.round(stats.undervalued / stats.total * 100);
                console.log(`   ${neighborhood}: ${stats.total} total (${stats.undervalued} deals, ${undervaluedPercent}%)`);
            });
        
        console.log('\n📋 NEXT STEPS:');
        console.log('   1. Check your Supabase "undervalued_rent_stabilized" table for all results');
        console.log('   2. Filter by market_classification:');
        console.log('      - "undervalued": High-priority targets (15%+ savings)');
        console.log('      - "moderately_undervalued": Good opportunities (5-15% savings)');
        console.log('      - "market_rate": Fair market deals');
        console.log('   3. Verify rent-stabilization status with landlord/DHCR before applying');
        console.log('   4. Contact undervalued listings immediately - good deals move fast!');
        
        console.log('\n🎯 API USAGE STATS:');
        console.log(`   Total API calls: ${this.apiUsageStats.totalCalls}`);
        console.log(`   Successful calls: ${this.apiUsageStats.successfulCalls}`);
        console.log(`   Failed calls: ${this.apiUsageStats.failedCalls}`);
        console.log(`   Cache efficiency: ${this.apiUsageStats.cacheHits} cache hits saved API calls`);
    }
}

/**
 * MAIN EXECUTION FUNCTION
 */
async function main() {
    try {
        const detector = new RentStabilizedDetector();
        
        // Check for test mode
        const testNeighborhood = process.env.TEST_NEIGHBORHOOD;
        if (testNeighborhood) {
            console.log(`🧪 TEST MODE: Analyzing ${testNeighborhood} only\n`);
            
            const results = await detector.findUndervaluedRentStabilizedListings({
                neighborhoods: [testNeighborhood],
                maxListingsPerNeighborhood: 100,
                testMode: true
            });
            
            console.log(`\n🎯 Test completed for ${testNeighborhood}`);
            console.log(`📊 Found ${results.rentStabilizedFound} rent-stabilized apartments`);
            console.log(`💾 Saved ${results.allRentStabilizedSaved} to undervalued_rent_stabilized table`);
            
            return results;
        }
        
        // Full production analysis
        console.log('🏙️ Running FULL NYC rent-stabilized analysis...\n');
        
        const results = await detector.findUndervaluedRentStabilizedListings({
            maxListingsPerNeighborhood: parseInt(process.env.MAX_LISTINGS_PER_NEIGHBORHOOD) || 500,
            testMode: false
        });
        
        console.log('\n🎉 Full NYC analysis completed!');
        console.log(`📊 Total rent-stabilized apartments found: ${results.rentStabilizedFound}`);
        console.log(`💾 All results saved to undervalued_rent_stabilized table`);
        console.log(`🎯 Check Supabase for complete listings with market classification`);
        
        return results;
        
    } catch (error) {
        console.error('💥 System crashed:', error.message);
        console.error('Stack trace:', error.stack);
        process.exit(1);
    }
}

// Export for use in other modules (railway-sequential-runner.js expects this)
module.exports = RentStabilizedDetector;

// Run if executed directly
if (require.main === module) {
    main().catch(error => {
        console.error('💥 Main execution failed:', error.message);
        console.error('Stack trace:', error.stack);
        process.exit(1);
    });
} '
