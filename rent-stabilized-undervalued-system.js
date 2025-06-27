// rent-stabilized-undervalued-system.js
// PRODUCTION-GRADE: Find rent-stabilized listings + save ALL with market classification
// WORKING VERSION: Uses original working API logic + comprehensive caching + saves ALL rent-stabilized
// SYNTAX FIXED: All deployment issues resolved for Railway

require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

// DHCR File parsing dependencies (install these)
const Papa = require('papaparse');     // npm install papaparse
const pdf = require('pdf-parse');      // npm install pdf-parse  
const XLSX = require('xlsx');          // npm install xlsx
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

        // STEP 2: Market classification thresholds (from .env)
        this.UNDERVALUATION_THRESHOLD = parseInt(process.env.UNDERVALUATION_THRESHOLD) || 15;
        this.MODERATE_UNDERVALUATION_THRESHOLD = 5; // 5-14.9% below market = moderately undervalued
        this.MARKET_RATE_THRESHOLD = 5; // Within ±5% = market rate
        this.OVERVALUED_THRESHOLD = -5; // More than 5% above market = overvalued
        
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
     * MAIN FUNCTION: Find rent-stabilized listings and save ALL with market classification
     */
    async findUndervaluedRentStabilizedListings(options = {}) {
        console.log('🏠 Finding ALL rent-stabilized listings (saving with market classification)...\n');

        const {
            neighborhoods = ['east-village', 'lower-east-side', 'chinatown'],
            maxListingsPerNeighborhood = parseInt(process.env.MAX_LISTINGS_PER_NEIGHBORHOOD) || 2000,
            testMode = false
        } = options;

        try {
            // Step 1: Get ALL listings in target neighborhoods (with comprehensive caching)
            console.log('📋 Step 1: Fetching all listings with comprehensive caching...');
            const allListings = await this.getAllListingsWithComprehensiveCaching(neighborhoods, maxListingsPerNeighborhood);
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

            // Step 4: Analyze ALL rent-stabilized listings and classify by market position
            console.log('💰 Step 4: Analyzing ALL rent-stabilized listings with market classification...');
            const analyzedStabilized = await this.analyzeAllRentStabilizedWithClassification(
                rentStabilizedListings,
                allListings
            );
            console.log(`   ✅ Analyzed rent-stabilized: ${analyzedStabilized.length}\n`);

            // Step 5: Save ALL results and generate report
            await this.saveAllResults(analyzedStabilized);
            this.generateComprehensiveReport(analyzedStabilized);

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
     * NEW: Get all listings with comprehensive caching for ALL listings
     */
    async getAllListingsWithComprehensiveCaching(neighborhoods, maxPerNeighborhood) {
        const allListings = [];
        
        for (const neighborhood of neighborhoods) {
            console.log(`   📍 Fetching ${neighborhood} with comprehensive caching...`);
            
            try {
                // STEP 1: Get existing cached listings for this neighborhood
                const cachedListings = await this.getComprehensiveCachedListings(neighborhood);
                console.log(`     💾 Found ${cachedListings.length} cached listings`);
                
                // STEP 2: Get fresh listings from StreetEasy (using WORKING API call)
                const freshListings = await this.fetchFreshListingsUsingWorkingAPI(neighborhood, maxPerNeighborhood);
                console.log(`     🆕 Fetched ${freshListings.length} fresh listings from API`);
                
                // STEP 3: Filter out listings we already have cached
                const cachedIds = new Set(cachedListings.map(l => l.id));
                const newListings = freshListings.filter(listing => !cachedIds.has(listing.id));
                console.log(`     ✨ ${newListings.length} new listings to cache`);
                
                // STEP 4: Cache ALL new listings (comprehensive caching)
                if (newListings.length > 0) {
                    await this.cacheAllListingsComprehensively(newListings);
                    console.log(`     ✅ Cached ${newListings.length} new listings`);
                }
                
                // STEP 5: Combine all listings for analysis
                const allNeighborhoodListings = [...cachedListings, ...newListings];
                allListings.push(...allNeighborhoodListings);
                
                const efficiency = freshListings.length > 0 ? 
                    ((cachedListings.length / (cachedListings.length + newListings.length)) * 100).toFixed(1) : 100;
                
                console.log(`     📊 Total: ${allNeighborhoodListings.length} listings (${efficiency}% cache efficiency)`);
                
            } catch (error) {
                console.error(`     ❌ Failed to fetch ${neighborhood}:`, error.message);
                continue;
            }
        }
        
        return allListings;
    }

    /**
     * NEW: Get cached listings from comprehensive_listing_cache
     */
    async getComprehensiveCachedListings(neighborhood) {
        try {
            // Try comprehensive_listing_cache first
            const { data: comprehensive, error: comprehensiveError } = await this.supabase
                .from('comprehensive_listing_cache')
                .select('*')
                .eq('neighborhood', neighborhood);
            
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
                .eq('neighborhood', neighborhood);
            
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
                .eq('neighborhood', neighborhood);
            
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
     * WORKING: Fetch fresh listings using the original working API call
     */
    async fetchFreshListingsUsingWorkingAPI(neighborhood, maxListings) {
        try {
            const rapidApiKey = process.env.RAPIDAPI_KEY;
            if (!rapidApiKey) {
                console.log(`       ⚠️ No RAPIDAPI_KEY found, cannot fetch from StreetEasy`);
                return [];
            }

            console.log(`       🌐 Fetching fresh listings from StreetEasy API for ${neighborhood}...`);
            
            const searchUrl = `https://streeteasy1.p.rapidapi.com/rentals/search`;
            
            // Use the WORKING API call structure from the original
            const response = await axios.get(searchUrl, {
                headers: {
                    'X-RapidAPI-Key': rapidApiKey,
                    'X-RapidAPI-Host': 'streeteasy1.p.rapidapi.com'
                },
                params: {
                    neighborhood: neighborhood,
                    limit: Math.min(maxListings, 500), // Respect API limits
                    offset: 0,
                    format: 'json'
                },
                timeout: 30000
            });

            if (response.data && response.data.rentals) {
                const listings = response.data.rentals.map(rental => ({
                    id: rental.id?.toString(),
                    address: rental.address || 'Unknown Address',
                    price: rental.price || 0,
                    bedrooms: rental.bedrooms || 0,
                    bathrooms: rental.bathrooms || 0,
                    sqft: rental.sqft || 0,
                    description: rental.description || '',
                    neighborhood: neighborhood,
                    amenities: rental.amenities || [],
                    url: rental.url || `https://streeteasy.com/rental/${rental.id}`,
                    listedAt: rental.listedAt || new Date().toISOString(),
                    source: 'streeteasy_api'
                }));
                
                console.log(`       ✅ StreetEasy API returned ${listings.length} fresh listings`);
                return listings;
            }

            console.log(`       ⚠️ StreetEasy API returned no rentals for ${neighborhood}`);
            return [];

        } catch (error) {
            console.error(`       ❌ StreetEasy API error for ${neighborhood}:`, error.message);
            return [];
        }
    }

    /**
     * NEW: Cache ALL listings to comprehensive_listing_cache
     */
    async cacheAllListingsComprehensively(listings) {
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
                console.error('Failed to cache to comprehensive table:', comprehensiveError.message);
                
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
     * STEP 3: Identify rent-stabilized listings using LEGAL INDICATORS ONLY
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
     * NEW: Analyze ALL rent-stabilized listings and classify by market position
     */
    async analyzeAllRentStabilizedWithClassification(rentStabilizedListings, allListings) {
        console.log(`   💰 Analyzing ${rentStabilizedListings.length} rent-stabilized listings (saving ALL)...\n`);
        
        const analyzedStabilized = [];
        
        for (const stabilizedListing of rentStabilizedListings) {
            console.log(`     📍 ${stabilizedListing.address}`);
            
            try {
                // Get market comparables (exclude other rent-stabilized units)
                const marketComparables = this.getMarketRateComparables(stabilizedListing, allListings);
                
                if (marketComparables.length < 5) {
                    console.log(`       ⚠️ Insufficient market comparables (${marketComparables.length})`);
                    // Still save it with basic classification
                    analyzedStabilized.push({
                        ...stabilizedListing,
                        estimatedMarketRent: stabilizedListing.price,
                        undervaluationPercent: 0,
                        potentialSavings: 0,
                        marketClassification: 'insufficient_data',
                        undervaluationMethod: 'insufficient_comparables',
                        undervaluationConfidence: 0,
                        comparablesUsed: marketComparables.length,
                        adjustments: []
                    });
                    continue;
                }
                
                // Run sophisticated undervaluation analysis
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
                
                // Classify market position and save ALL rent-stabilized listings
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
                
                // Log different messages based on classification
                if (marketClassification === 'undervalued') {
                    console.log(`       ✅ UNDERVALUED! Savings: $${savings.toLocaleString()}/month`);
                } else if (marketClassification === 'moderately_undervalued') {
                    console.log(`       📊 MODERATELY UNDERVALUED: $${savings.toLocaleString()}/month`);
                } else if (marketClassification === 'overvalued') {
                    const premium = Math.abs(undervaluationAnalysis.estimatedMarketRent - stabilizedListing.price);
                    console.log(`       📈 ABOVE MARKET: $${premium.toLocaleString()}/month premium`);
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
     * NEW: Classify market position using all thresholds
     */
    classifyMarketPosition(percentBelowMarket) {
        if (percentBelowMarket >= this.UNDERVALUATION_THRESHOLD) {
            return 'undervalued';      // 15%+ below market (or whatever threshold is set)
        } else if (percentBelowMarket >= this.MODERATE_UNDERVALUATION_THRESHOLD) {
            return 'moderately_undervalued'; // 5-14.9% below market  
        } else if (percentBelowMarket >= this.OVERVALUED_THRESHOLD) {
            return 'market_rate';      // -5% to +14.9% (within reasonable range of market)
        } else {
            return 'overvalued';       // More than 5% above market (< -5%)
        }
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
     * SIMPLIFIED: Load rent-stabilized buildings from database only
     */
    async loadRentStabilizedBuildings() {
        try {
            const { data, error } = await this.supabase
                .from('rent_stabilized_buildings')
                .select('500000');
            
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
     * Analyze building legal criteria for rent stabilization - FIXED CONFIDENCE SCORING
     */
    analyzeBuildingLegalCriteria(listing, building) {
        let confidence = 0;
        const factors = [];
        
        // DHCR registration status (STRONGEST indicator)
        if (building.dhcr_registered) {
            confidence += 50; // INCREASED: DHCR registration is nearly definitive
            factors.push('DHCR registered building');
        }
        
        // Building age criteria (LEGAL requirement)
        const buildYear = building.year_built || this.extractYearFromDescription(listing.description);
        if (buildYear) {
            if (buildYear < 1947) {
                confidence += 30; // INCREASED: Pre-war buildings have strong stabilization
                factors.push('Pre-1947 building (pre-war)');
            } else if (buildYear <= 1973) {
                confidence += 40; // INCREASED: Golden age of rent stabilization
                factors.push('1947-1973 building (post-war regulated era)');
            } else if (buildYear <= 1984) {
                confidence += 15; // NEW: Some post-1974 buildings with tax benefits
                factors.push('1974-1984 building (may have tax benefits)');
            }
        }
        
        // Unit count criteria (LEGAL requirement)
        if (building.total_units >= 6) {
            confidence += 25; // INCREASED: 6+ units is a legal requirement
            factors.push('6+ units (meets minimum for stabilization)');
        } else if (building.total_units >= 4) {
            confidence += 10; // NEW: Some 4-5 unit buildings may qualify
            factors.push('4-5 units (borderline for stabilization)');
        }
        
        // Building type indicators
        if (building.building_class && building.building_class.includes('MULTIPLE DWELLING')) {
            confidence += 10; // NEW: Multiple dwelling classification
            factors.push('Multiple dwelling classification');
        }
        
        // Neighborhood-based likelihood (NYC context)
        if (listing.neighborhood) {
            const highStabilizationNeighborhoods = [
                'east-village', 'lower-east-side', 'upper-west-side', 'upper-east-side',
                'chelsea', 'greenwich-village', 'murray-hill', 'gramercy'
            ];
            
            if (highStabilizationNeighborhoods.includes(listing.neighborhood.toLowerCase())) {
                confidence += 10; // NEW: High-stabilization neighborhoods
                factors.push('High rent-stabilization neighborhood');
            }
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
     * NEW: Save ALL results with market classification
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
     * NEW: Generate comprehensive report with market classifications
     */
    generateComprehensiveReport(analyzedStabilized) {
        console.log('\n🎉 COMPREHENSIVE RENT-STABILIZED LISTINGS REPORT');
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
            maxListingsPerNeighborhood: parseInt(process.env.MAX_LISTINGS_PER_NEIGHBORHOOD) || 2000,
            testMode: false
        });
        
        console.log('\n🎉 Analysis complete!');
        console.log(`📊 Check your Supabase 'undervalued_rent_stabilized' table for ${results.allRentStabilizedSaved} listings`);
        console.log(`🎯 All rent-stabilized listings saved with market classification`);
        
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
