// rent-stabilized-undervalued-system.js
// COMPLETE DUAL-ANALYSIS SYSTEM: Rent-Stabilized Detection + Advanced Undervaluation Analysis
// Implements ALL required functions for production-ready StreetEasy scraping with comprehensive valuation
require('dotenv').config();
const axios = require('axios');
const { createClient } = require('@supabase/supabase-js');
const fs = require('fs');
const path = require('path');

/**
 * COMPLETE NYC Rent-Stabilized & Undervaluation Detection System
 * Combines legal rent-stabilization indicators with sophisticated market valuation
 */
class RentStabilizedUndervaluedDetector {
    constructor() {
        // Initialize Supabase client
        this.supabase = createClient(
            process.env.SUPABASE_URL,
            process.env.SUPABASE_ANON_KEY
        );

        // Configuration from environment
        this.rapidApiKey = process.env.RAPIDAPI_KEY;
        this.testNeighborhood = process.env.TEST_NEIGHBORHOOD;
        this.confidenceThreshold = parseInt(process.env.RENT_STABILIZED_CONFIDENCE_THRESHOLD) || 40;
        this.undervaluationThreshold = parseFloat(process.env.UNDERVALUATION_THRESHOLD) || -100;
        this.maxListingsPerNeighborhood = parseInt(process.env.MAX_LISTINGS_PER_NEIGHBORHOOD) || 2000;

        // Rate limiting
        this.baseDelay = 1200; // 1.2 seconds between calls
        this.apiCallsUsed = 0;

        // Advanced valuation configuration
        this.VALUATION_METHODS = {
            EXACT_MATCH: 'exact_bed_bath_amenity_match',
            BED_BATH_SPECIFIC: 'bed_bath_specific_pricing',
            BED_SPECIFIC: 'bed_specific_with_adjustments',
            PRICE_PER_SQFT_FALLBACK: 'price_per_sqft_fallback'
        };

        this.MIN_SAMPLES = {
            EXACT_MATCH: 3,
            BED_BATH_SPECIFIC: 8,
            BED_SPECIFIC: 12,
            PRICE_PER_SQFT_FALLBACK: 20
        };

        // NYC Borough mapping
        this.BOROUGH_MAP = {
            'manhattan': ['west-village', 'east-village', 'soho', 'tribeca', 'chelsea', 'upper-east-side', 'upper-west-side', 'midtown', 'lower-east-side', 'greenwich-village', 'nolita', 'chinatown', 'financial-district'],
            'brooklyn': ['park-slope', 'williamsburg', 'dumbo', 'brooklyn-heights', 'fort-greene', 'prospect-heights', 'crown-heights', 'bedford-stuyvesant', 'greenpoint', 'bushwick', 'red-hook', 'cobble-hill'],
            'queens': ['long-island-city', 'astoria', 'sunnyside', 'jackson-heights', 'elmhurst', 'flushing', 'forest-hills', 'woodside'],
            'bronx': ['south-bronx', 'mott-haven', 'concourse', 'fordham', 'riverdale']
        };

        // NYC amenity adjustments by borough
        this.AMENITY_ADJUSTMENTS = {
            manhattan: {
                'elevator': 150, 'doorman': 300, 'gym': 200, 'roof_deck': 250,
                'laundry': 100, 'parking': 400, 'balcony': 200, 'dishwasher': 75,
                'air_conditioning': 100, 'hardwood': 150, 'concierge': 250
            },
            brooklyn: {
                'elevator': 100, 'doorman': 200, 'gym': 150, 'roof_deck': 200,
                'laundry': 75, 'parking': 300, 'balcony': 150, 'dishwasher': 50,
                'air_conditioning': 75, 'hardwood': 100, 'concierge': 150
            },
            queens: {
                'elevator': 75, 'doorman': 150, 'gym': 100, 'roof_deck': 125,
                'laundry': 50, 'parking': 200, 'balcony': 100, 'dishwasher': 40,
                'air_conditioning': 50, 'hardwood': 75, 'concierge': 100
            },
            bronx: {
                'elevator': 50, 'doorman': 100, 'gym': 75, 'roof_deck': 100,
                'laundry': 40, 'parking': 150, 'balcony': 75, 'dishwasher': 30,
                'air_conditioning': 40, 'hardwood': 50, 'concierge': 75
            }
        };

        console.log('🏢 NYC Rent-Stabilized & Undervaluation Detector initialized');
        console.log(`📊 Confidence threshold: ${this.confidenceThreshold}%`);
        console.log(`💰 Undervaluation threshold: ${this.undervaluationThreshold}%`);
    }

    /**
     * MAIN FUNCTION: Find undervalued rent-stabilized listings
     */
    async findUndervaluedRentStabilizedListings(options = {}) {
        try {
            console.log('\n🚀 Starting NYC Rent-Stabilized & Undervaluation Analysis...\n');

            // Step 1: Get all neighborhoods to analyze
            const neighborhoods = await this.getNeighborhoodsToAnalyze(options);
            console.log(`🗽 Analyzing ${neighborhoods.length} NYC neighborhoods...\n`);

            // Step 2: Get all listings with intelligent caching
            console.log('📊 Step 1: Fetching all listings with comprehensive caching...');
            const allListings = await this.getAllListingsWithCorrectedCaching(neighborhoods, this.maxListingsPerNeighborhood);
            console.log(`   ✅ Total listings: ${allListings.length}\n`);

            // Step 3: Load rent-stabilized buildings database
            console.log('🏢 Step 2: Loading rent-stabilized buildings...');
            const stabilizedBuildings = await this.loadRentStabilizedBuildingsEnhanced();
            console.log(`   ✅ Stabilized buildings: ${stabilizedBuildings.length}\n`);

            // Step 4: Find rent-stabilized listings using multiple indicators
            console.log('⚖️ Step 3: Identifying rent-stabilized listings...');
            const rentStabilizedListings = await this.identifyRentStabilizedListings(allListings, stabilizedBuildings);
            console.log(`   ✅ Rent-stabilized found: ${rentStabilizedListings.length}\n`);

            // Step 5: Analyze ALL rent-stabilized listings with advanced valuation
            console.log('💰 Step 4: Advanced undervaluation analysis...');
            const analyzedStabilized = await this.analyzeAllRentStabilizedWithAdvancedClassification(rentStabilizedListings, allListings);
            console.log(`   ✅ Analyzed rent-stabilized: ${analyzedStabilized.length}\n`);

            // Step 6: Save ALL results and generate comprehensive report
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
     * Get neighborhoods to analyze based on DHCR data or test mode
     */
    async getNeighborhoodsToAnalyze(options) {
        if (this.testNeighborhood) {
            console.log(`🧪 Test mode: analyzing only ${this.testNeighborhood}`);
            return [this.testNeighborhood];
        }

        if (options.neighborhoods && options.neighborhoods.length > 0) {
            return options.neighborhoods;
        }

        // Default NYC neighborhoods with rent-stabilized potential
        return [
            'east-village', 'lower-east-side', 'chinatown', 'greenwich-village',
            'chelsea', 'midtown', 'upper-east-side', 'upper-west-side',
            'park-slope', 'fort-greene', 'prospect-heights', 'crown-heights',
            'bedford-stuyvesant', 'williamsburg', 'greenpoint', 'bushwick',
            'long-island-city', 'astoria', 'sunnyside', 'jackson-heights'
        ];
    }

    /**
     * CORE: Get all listings with comprehensive caching system
     */
    async getAllListingsWithCorrectedCaching(neighborhoods, maxPerNeighborhood) {
        const allListings = [];
        
        for (const neighborhood of neighborhoods) {
            console.log(`   📍 Processing ${neighborhood}...`);
            
            try {
                const neighborhoodListings = await this.processNeighborhood(neighborhood, maxPerNeighborhood);
                allListings.push(...neighborhoodListings);
                
                console.log(`   ✅ ${neighborhood}: ${neighborhoodListings.length} listings`);
                
                // Rate limiting between neighborhoods
                await this.delay(this.baseDelay);
                
            } catch (error) {
                console.error(`   ❌ Failed to process ${neighborhood}:`, error.message);
            }
        }
        
        return allListings;
    }

    /**
     * Process single neighborhood with caching
     */
    async processNeighborhood(neighborhood, maxListings) {
        // Step 1: Get cached listings
        const cachedListings = await this.getCachedListingsWithFullDetails(neighborhood);
        console.log(`     💾 Cached: ${cachedListings.length}`);

        // Step 2: Fetch fresh basic listing IDs
        const basicListingIds = await this.fetchBasicListingIds(neighborhood, maxListings);
        console.log(`     🆕 Fresh IDs: ${basicListingIds.length}`);

        // Step 3: Filter out already cached listings
        const cachedIds = new Set(cachedListings.map(l => l.id));
        const newIds = basicListingIds.filter(item => !cachedIds.has(item.id));
        console.log(`     ✨ New listings: ${newIds.length}`);

        // Step 4: Fetch individual listing details for new listings
        const newDetailedListings = [];
        for (const basicItem of newIds) {
            try {
                const detailListing = await this.fetchIndividualListingDetails(basicItem.id);
                if (detailListing && detailListing.id) {
                    newDetailedListings.push({
                        ...basicItem,
                        ...detailListing,
                        neighborhood: neighborhood
                    });
                }
                await this.delay(this.baseDelay); // Rate limiting
            } catch (error) {
                console.error(`     ❌ Failed to fetch details for ${basicItem.id}:`, error.message);
            }
        }

        // Step 5: Cache new listings with full details
        if (newDetailedListings.length > 0) {
            await this.cacheListingsWithFullDetails(newDetailedListings);
            console.log(`     💾 Cached ${newDetailedListings.length} new listings`);
        }

        // Step 6: Return combined listings
        return [...cachedListings, ...newDetailedListings];
    }

    /**
     * Get cached listings with full details
     */
    async getCachedListingsWithFullDetails(neighborhood) {
        try {
            const { data, error } = await this.supabase
                .from('comprehensive_listing_cache')
                .select('*')
                .eq('neighborhood', neighborhood)
                .gte('cached_at', new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString()); // 30 days

            if (error) throw error;

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
                buildingAmenities: row.building_amenities || [],
                url: row.listing_url || `https://streeteasy.com/rental/${row.listing_id}`,
                yearBuilt: row.year_built,
                buildingType: row.building_type,
                totalUnits: row.total_units_in_building,
                brokerFee: row.broker_fee,
                availableDate: row.available_date,
                source: 'cache'
            }));
            
        } catch (error) {
            console.error('Failed to get cached listings:', error.message);
            return [];
        }
    }

    /**
     * Fetch basic listing IDs from StreetEasy API
     */
    async fetchBasicListingIds(neighborhood, maxListings) {
        try {
            if (!this.rapidApiKey) {
                console.log(`       ⚠️ No RAPIDAPI_KEY found, cannot fetch from StreetEasy`);
                return [];
            }

            console.log(`       🌐 Fetching basic listing IDs for ${neighborhood}...`);
            
            const response = await axios.get('https://streeteasy1.p.rapidapi.com/rentals/search', {
                headers: {
                    'X-RapidAPI-Key': this.rapidApiKey,
                    'X-RapidAPI-Host': 'streeteasy1.p.rapidapi.com'
                },
                params: {
                    neighborhood: neighborhood,
                    limit: Math.min(maxListings, 500),
                    offset: 0,
                    format: 'json'
                },
                timeout: 30000
            });

            this.apiCallsUsed++;

            if (response.data && response.data.rentals) {
                return response.data.rentals.map(rental => ({
                    id: rental.id?.toString(),
                    address: rental.address || 'Unknown Address',
                    price: rental.price || 0,
                    neighborhood: neighborhood
                }));
            }

            return [];

        } catch (error) {
            console.error(`       ❌ StreetEasy API error for ${neighborhood}:`, error.message);
            return [];
        }
    }

    /**
     * Fetch individual listing details with full data
     */
    async fetchIndividualListingDetails(listingId) {
        try {
            if (!this.rapidApiKey) return null;

            const response = await axios.get(`https://streeteasy1.p.rapidapi.com/rentals/${listingId}`, {
                headers: {
                    'X-RapidAPI-Key': this.rapidApiKey,
                    'X-RapidAPI-Host': 'streeteasy1.p.rapidapi.com'
                },
                timeout: 30000
            });

            this.apiCallsUsed++;

            const data = response.data;
            if (!data) return null;

            return {
                id: data.id?.toString(),
                address: data.address || 'Unknown Address',
                price: data.price || 0,
                bedrooms: data.bedrooms || 0,
                bathrooms: data.bathrooms || 0,
                sqft: data.sqft || 0,
                description: data.description || '',
                amenities: data.amenities || [],
                buildingAmenities: data.buildingAmenities || [],
                yearBuilt: this.extractYearBuilt(data.description || ''),
                buildingType: data.buildingType || 'apartment',
                totalUnits: data.totalUnits || 0,
                brokerFee: data.brokerFee || '',
                availableDate: data.availableDate || null,
                leaseTerms: data.leaseTerms || '',
                petPolicy: data.petPolicy || '',
                brokerName: data.brokerName || '',
                brokerPhone: data.brokerPhone || '',
                images: data.images || [],
                virtualTourUrl: data.virtualTourUrl || '',
                url: data.url || `https://streeteasy.com/rental/${listingId}`
            };

        } catch (error) {
            console.error(`Failed to fetch details for listing ${listingId}:`, error.message);
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
                amenities: listing.amenities || [],
                building_amenities: listing.buildingAmenities || [],
                building_type: listing.buildingType,
                year_built: listing.yearBuilt,
                total_units_in_building: listing.totalUnits,
                broker_fee: listing.brokerFee,
                available_date: listing.availableDate,
                listing_url: listing.url,
                cached_at: new Date().toISOString()
            }));

            const { error } = await this.supabase
                .from('comprehensive_listing_cache')
                .upsert(cacheData, { onConflict: 'listing_id' });

            if (error) throw error;

        } catch (error) {
            console.error('Failed to cache listings:', error.message);
        }
    }

    /**
     * Rate limiting delay
     */
    async delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    /**
     * Load rent-stabilized buildings from DHCR data
     */
    async loadRentStabilizedBuildingsEnhanced() {
        try {
            const { data, error } = await this.supabase
                .from('rent_stabilized_buildings')
                .select('*')
                .limit(5000);

            if (error) throw error;

            return data || [];
            
        } catch (error) {
            console.error('Failed to load rent-stabilized buildings:', error.message);
            return [];
        }
    }

    /**
     * Identify rent-stabilized listings using multiple indicators
     */
    async identifyRentStabilizedListings(allListings, stabilizedBuildings) {
        const rentStabilizedListings = [];
        
        for (const listing of allListings) {
            const analysis = this.analyzeRentStabilizationWithAllIndicators(listing, stabilizedBuildings);
            
            if (analysis.confidence >= this.confidenceThreshold) {
                rentStabilizedListings.push({
                    ...listing,
                    rentStabilizedConfidence: analysis.confidence,
                    rentStabilizedMethod: analysis.method,
                    rentStabilizationAnalysis: {
                        explanation: analysis.explanation,
                        key_factors: analysis.factors,
                        probability: analysis.confidence,
                        legal_indicators: analysis.legalIndicators || [],
                        building_criteria: analysis.buildingCriteria || {},
                        dhcr_building_match: analysis.dhcrMatch || false,
                        confidence_breakdown: analysis.confidenceBreakdown || {}
                    }
                });
                
                console.log(`   ✅ Found: ${listing.address} (${analysis.confidence}% confidence)`);
            }
        }
        
        return rentStabilizedListings;
    }

    /**
     * Analyze rent stabilization with all indicators
     */
    analyzeRentStabilizationWithAllIndicators(listing, stabilizedBuildings) {
        let confidence = 0;
        let method = 'circumstantial';
        const factors = [];
        const legalIndicators = [];
        let explanation = '';

        // Method 1: Explicit mention in description (STRONGEST)
        const description = (listing.description || '').toLowerCase();
        const explicitKeywords = [
            'rent stabilized', 'rent-stabilized', 'stabilized unit',
            'dhcr', 'legal rent', 'preferential rent'
        ];
        
        for (const keyword of explicitKeywords) {
            if (description.includes(keyword)) {
                confidence += 50;
                method = 'explicit_mention';
                factors.push(`Explicit mention: "${keyword}"`);
                legalIndicators.push(keyword);
                explanation = `Strong legal indicator found in listing description`;
            }
        }

        // Method 2: DHCR building registration match
        const buildingMatch = this.findMatchingStabilizedBuilding(listing, stabilizedBuildings);
        if (buildingMatch) {
            confidence += 40;
            if (method === 'circumstantial') method = 'dhcr_registered';
            factors.push(`DHCR building registration match`);
            legalIndicators.push('dhcr_registration');
            explanation = explanation || `Building found in DHCR rent-stabilized registry`;
        }

        // Method 3: Building age analysis (6+ units built before 1974)
        const buildingAge = this.analyzeBuildingAge(listing);
        if (buildingAge.isOldEnough && buildingAge.hasEnoughUnits) {
            confidence += 25;
            if (method === 'circumstantial') method = 'building_analysis';
            factors.push(`Pre-1974 building with ${buildingAge.units || '6+'} units`);
            explanation = explanation || `Building meets age and size criteria for rent stabilization`;
        }

        // Method 4: Circumstantial indicators
        const circumstantialKeywords = [
            'no broker fee', 'long term tenant', 'below market',
            'rent controlled', 'housing court', 'tenant rights'
        ];
        
        for (const keyword of circumstantialKeywords) {
            if (description.includes(keyword)) {
                confidence += 5;
                factors.push(`Circumstantial: "${keyword}"`);
            }
        }

        return {
            confidence: Math.min(100, Math.round(confidence)),
            method,
            factors,
            explanation,
            legalIndicators,
            dhcrMatch: !!buildingMatch,
            buildingCriteria: buildingAge,
            confidenceBreakdown: {
                explicit_mention: explicitKeywords.some(k => description.includes(k)) ? 50 : 0,
                dhcr_registration: buildingMatch ? 40 : 0,
                building_analysis: (buildingAge.isOldEnough && buildingAge.hasEnoughUnits) ? 25 : 0,
                circumstantial: Math.min(25, factors.filter(f => f.startsWith('Circumstantial')).length * 5)
            }
        };
    }

    /**
     * Find matching stabilized building in DHCR data
     */
    findMatchingStabilizedBuilding(listing, stabilizedBuildings) {
        if (!stabilizedBuildings || stabilizedBuildings.length === 0) return null;
        
        const normalizedAddress = this.normalizeAddress(listing.address);
        
        return stabilizedBuildings.find(building => {
            const buildingAddress = this.normalizeAddress(building.address || '');
            return buildingAddress === normalizedAddress;
        });
    }

    /**
     * Analyze building age and unit count for rent stabilization eligibility
     */
    analyzeBuildingAge(listing) {
        const yearBuilt = listing.yearBuilt || this.extractYearBuilt(listing.description || '');
        const totalUnits = listing.totalUnits || 0;
        
        const isOldEnough = yearBuilt && yearBuilt < 1974;
        const hasEnoughUnits = totalUnits >= 6 || totalUnits === 0; // 0 means unknown, assume eligible
        
        return {
            yearBuilt,
            units: totalUnits,
            isOldEnough,
            hasEnoughUnits,
            eligible: isOldEnough && hasEnoughUnits
        };
    }

    /**
     * Normalize address for matching
     */
    normalizeAddress(address) {
        return address
            .toLowerCase()
            .replace(/[^\w\s]/g, '')
            .replace(/\s+/g, ' ')
            .trim();
    }

    /**
     * Extract year built from description
     */
    extractYearBuilt(description) {
        const yearMatch = description.match(/\b(19\d{2}|20[0-2]\d)\b/);
        return yearMatch ? parseInt(yearMatch[1]) : null;
    }

    /**
     * ADVANCED: Analyze ALL rent-stabilized listings with sophisticated classification
     */
    async analyzeAllRentStabilizedWithAdvancedClassification(rentStabilizedListings, allListings) {
        console.log(`   💰 Analyzing ${rentStabilizedListings.length} rent-stabilized listings (saving ALL)...\n`);
        
        const analyzedStabilized = [];
        
        for (const stabilizedListing of rentStabilizedListings) {
            console.log(`     📍 ${stabilizedListing.address}`);
            
            try {
                // Get market comparables (exclude other rent-stabilized units)
                const marketComparables = this.getMarketRateComparables(stabilizedListing, allListings);
                
                if (marketComparables.length < 5) {
                    console.log(`       ⚠️ Insufficient market comparables (${marketComparables.length})`);
                    
                    // Still save with basic classification
                    analyzedStabilized.push({
                        ...stabilizedListing,
                        estimatedMarketRent: stabilizedListing.price,
                        undervaluationPercent: 0,
                        potentialSavings: 0,
                        marketClassification: 'insufficient_data',
                        undervaluationMethod: 'insufficient_comparables',
                        undervaluationConfidence: 0,
                        comparablesUsed: marketComparables.length,
                        undervaluationAnalysis: {
                            adjustments: [],
                            methodology: 'insufficient_data',
                            base_market_rent: stabilizedListing.price,
                            calculation_steps: ['Insufficient comparable properties'],
                            total_adjustments: 0,
                            confidence_factors: { data_quality: 0 },
                            comparable_properties: [],
                            final_market_estimate: stabilizedListing.price
                        }
                    });
                    continue;
                }
                
                // Run sophisticated undervaluation analysis
                const undervaluationAnalysis = await this.analyzeUndervaluationFixed(
                    stabilizedListing,
                    marketComparables
                );
                
                if (!undervaluationAnalysis.success) {
                    console.log(`       ❌ Undervaluation analysis failed`);
                    
                    // Still save with basic data
                    analyzedStabilized.push({
                        ...stabilizedListing,
                        estimatedMarketRent: stabilizedListing.price,
                        undervaluationPercent: 0,
                        potentialSavings: 0,
                        marketClassification: 'analysis_failed',
                        undervaluationMethod: 'failed',
                        undervaluationConfidence: 0,
                        comparablesUsed: marketComparables.length,
                        undervaluationAnalysis: {
                            adjustments: [],
                            methodology: 'analysis_failed',
                            base_market_rent: stabilizedListing.price,
                            calculation_steps: ['Analysis failed'],
                            total_adjustments: 0,
                            confidence_factors: { error: true },
                            comparable_properties: [],
                            final_market_estimate: stabilizedListing.price
                        }
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
                    undervaluationAnalysis: {
                        adjustments: undervaluationAnalysis.adjustments || [],
                        methodology: undervaluationAnalysis.method,
                        base_market_rent: undervaluationAnalysis.baseMarketRent,
                        calculation_steps: undervaluationAnalysis.calculationSteps || [],
                        total_adjustments: undervaluationAnalysis.totalAdjustments || 0,
                        confidence_factors: undervaluationAnalysis.confidenceFactors || {},
                        comparable_properties: marketComparables.slice(0, 10), // Store first 10 comparables
                        final_market_estimate: undervaluationAnalysis.estimatedMarketRent
                    }
                });
                
                // Log different messages based on classification
                if (marketClassification === 'undervalued') {
                    console.log(`       ✅ UNDERVALUED! ${percentBelowMarket.toFixed(1)}% below market`);
                } else if (marketClassification === 'moderately_undervalued') {
                    console.log(`       💡 Moderately undervalued: ${percentBelowMarket.toFixed(1)}% below market`);
                } else {
                    console.log(`       📊 ${marketClassification}: ${percentBelowMarket.toFixed(1)}% vs market`);
                }
                
            } catch (error) {
                console.error(`     ❌ Analysis failed for ${stabilizedListing.address}:`, error.message);
            }
        }
        
        return analyzedStabilized;
    }

    /**
     * Get market rate comparables (excluding rent-stabilized listings)
     */
    getMarketRateComparables(targetListing, allListings) {
        const targetNeighborhood = targetListing.neighborhood;
        const targetBeds = targetListing.bedrooms || 0;
        const targetBaths = targetListing.bathrooms || 0;
        
        // Get adjacent neighborhoods for broader comparison
        const adjacentNeighborhoods = this.getAdjacentNeighborhoods(targetNeighborhood);
        const searchNeighborhoods = [targetNeighborhood, ...adjacentNeighborhoods];
        
        return allListings.filter(listing => {
            // Exclude the target listing itself
            if (listing.id === targetListing.id) return false;
            
            // Exclude obvious rent-stabilized listings
            if (this.appearsRentStabilized(listing)) return false;
            
            // Include listings from target and adjacent neighborhoods
            if (!searchNeighborhoods.includes(listing.neighborhood)) return false;
            
            // Prefer similar bedroom count (allow +/- 1)
            const bedDiff = Math.abs((listing.bedrooms || 0) - targetBeds);
            if (bedDiff > 1) return false;
            
            // Prefer similar bathroom count (allow more flexibility)
            const bathDiff = Math.abs((listing.bathrooms || 0) - targetBaths);
            if (bathDiff > 1.5) return false;
            
            // Must have valid price
            if (!listing.price || listing.price <= 0) return false;
            
            return true;
        }).sort((a, b) => {
            // Sort by bedroom similarity first, then bathroom similarity
            const bedDiffA = Math.abs((a.bedrooms || 0) - targetBeds);
            const bedDiffB = Math.abs((b.bedrooms || 0) - targetBeds);
            if (bedDiffA !== bedDiffB) return bedDiffA - bedDiffB;
            
            const bathDiffA = Math.abs((a.bathrooms || 0) - targetBaths);
            const bathDiffB = Math.abs((b.bathrooms || 0) - targetBaths);
            return bathDiffA - bathDiffB;
        });
    }

    /**
     * Get adjacent neighborhoods for broader market comparison
     */
    getAdjacentNeighborhoods(neighborhood) {
        const adjacencyMap = {
            'east-village': ['lower-east-side', 'greenwich-village', 'nolita'],
            'lower-east-side': ['east-village', 'chinatown', 'financial-district'],
            'greenwich-village': ['east-village', 'west-village', 'soho'],
            'west-village': ['greenwich-village', 'chelsea', 'tribeca'],
            'soho': ['nolita', 'tribeca', 'greenwich-village'],
            'chelsea': ['west-village', 'midtown', 'gramercy'],
            'upper-east-side': ['upper-west-side', 'midtown'],
            'upper-west-side': ['upper-east-side', 'midtown'],
            'park-slope': ['prospect-heights', 'fort-greene', 'carroll-gardens'],
            'williamsburg': ['greenpoint', 'bedford-stuyvesant', 'dumbo'],
            'astoria': ['long-island-city', 'sunnyside', 'jackson-heights']
        };
        
        return adjacencyMap[neighborhood] || [];
    }

    /**
     * Quick check if listing appears rent-stabilized
     */
    appearsRentStabilized(listing) {
        const description = (listing.description || '').toLowerCase();
        const rentStabilizedKeywords = [
            'rent stabilized', 'rent-stabilized', 'stabilized unit',
            'dhcr', 'legal rent', 'preferential rent'
        ];
        
        return rentStabilizedKeywords.some(keyword => description.includes(keyword));
    }

    /**
     * Classify market position based on undervaluation percentage
     */
    classifyMarketPosition(percentBelowMarket) {
        if (percentBelowMarket >= 15) {
            return 'undervalued'; // 15%+ below market
        } else if (percentBelowMarket >= 5) {
            return 'moderately_undervalued'; // 5-15% below market
        } else if (percentBelowMarket >= -5) {
            return 'market_rate'; // Within 5% of market rate
        } else {
            return 'overvalued'; // Above market rate
        }
    }

    /**
     * CORE MISSING FUNCTION: Advanced undervaluation analysis
     */
    async analyzeUndervaluationFixed(targetListing, marketComparables) {
        try {
            console.log(`       🧠 Advanced valuation for ${targetListing.address}...`);
            
            // STEP 1: Select best valuation method
            const valuationResult = this.selectAdvancedValuationMethodFixed(targetListing, marketComparables);
            
            if (!valuationResult.success) {
                return { success: false, reason: valuationResult.reason };
            }
            
            console.log(`       📊 Using ${valuationResult.method} with ${valuationResult.comparables.length} comparables`);
            
            // STEP 2: Calculate base market value
            const baseMarketValue = this.calculateAdvancedBaseMarketValue(
                targetListing,
                valuationResult.comparables,
                valuationResult.method
            );
            
            // STEP 3: Apply sophisticated adjustments
            const adjustedMarketValue = this.applyAdvancedMarketAdjustments(
                targetListing,
                baseMarketValue,
                valuationResult.comparables,
                valuationResult.method
            );
            
            // STEP 4: Calculate confidence score
            const confidence = this.calculateAdvancedConfidenceScore(
                targetListing,
                valuationResult.comparables,
                valuationResult.method
            );
            
            // STEP 5: Calculate undervaluation metrics
            const estimatedMarketRent = adjustedMarketValue.finalValue;
            const percentBelowMarket = ((estimatedMarketRent - targetListing.price) / estimatedMarketRent) * 100;
            
            // STEP 6: Generate reasoning
            const reasoning = this.generateAdvancedValuationReasoning(
                targetListing,
                baseMarketValue,
                adjustedMarketValue,
                valuationResult
            );
            
            console.log(`       💰 Base: ${baseMarketValue.baseValue.toLocaleString()}`);
            console.log(`       🔧 Adjustments: ${adjustedMarketValue.totalAdjustments >= 0 ? '+' : ''}${adjustedMarketValue.totalAdjustments.toLocaleString()}`);
            console.log(`       🎯 Est. market rent: ${estimatedMarketRent.toLocaleString()}`);
            console.log(`       📊 Method: ${valuationResult.method} (${confidence}% confidence)`);

            return {
                success: true,
                estimatedMarketRent: estimatedMarketRent,
                baseMarketRent: baseMarketValue.baseValue,
                totalAdjustments: adjustedMarketValue.totalAdjustments,
                adjustments: adjustedMarketValue.adjustments,
                method: valuationResult.method,
                confidence: confidence,
                comparablesUsed: valuationResult.comparables.length,
                percentBelowMarket: percentBelowMarket,
                calculationSteps: reasoning.steps,
                confidenceFactors: reasoning.confidenceFactors
            };

        } catch (error) {
            console.error(`       ❌ Valuation failed:`, error.message);
            return { success: false, reason: error.message };
        }
    }

    /**
     * MISSING FUNCTION: Select advanced valuation method
     */
    selectAdvancedValuationMethodFixed(targetProperty, comparables) {
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

        // Method 2: Same bed/bath count (broader tolerance)
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
     * MISSING FUNCTION: Calculate advanced base market value
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
     * MISSING FUNCTION: Method 1 & 2 - Bed/Bath specific pricing
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
     * MISSING FUNCTION: Method 3 - Bedroom-based with bathroom adjustments
     */
    calculateBedroomBasedValueWithBathAdjustments(targetProperty, comparables) {
        const targetBaths = targetProperty.bathrooms || 1;
        
        // Calculate base rent for this bedroom count
        const rents = comparables.map(comp => comp.price);
        const medianRent = this.calculateMedian(rents);
        
        // Find typical bathroom count for this bedroom category
        const bathCounts = comparables.map(comp => comp.bathrooms || 1);
        const typicalBaths = this.calculateMedian(bathCounts);
        
        // Calculate bathroom adjustment
        const bathDifference = targetBaths - typicalBaths;
        const bathAdjustment = this.calculateBathroomAdjustment(bathDifference, targetProperty.borough);
        
        return {
            baseValue: Math.round(medianRent + bathAdjustment),
            method: 'bed_median_with_bath_adjustment',
            dataPoints: rents.length,
            bathAdjustment: bathAdjustment,
            rentRange: { min: Math.min(...rents), max: Math.max(...rents) }
        };
    }

    /**
     * MISSING FUNCTION: Method 4 - Square footage based valuation
     */
    calculateSqftBasedValue(targetProperty, comparables) {
        const targetSqft = targetProperty.sqft || this.estimateSquareFootage(targetProperty);
        
        // Calculate price per sqft from comparables
        const pricePerSqftValues = comparables
            .filter(comp => comp.sqft > 0 && comp.price > 0)
            .map(comp => comp.price / comp.sqft);
        
        const medianPricePerSqft = this.calculateMedian(pricePerSqftValues);
        const estimatedRent = Math.round(medianPricePerSqft * targetSqft);
        
        return {
            baseValue: estimatedRent,
            method: 'price_per_sqft',
            dataPoints: pricePerSqftValues.length,
            pricePerSqft: medianPricePerSqft,
            estimatedSqft: targetSqft
        };
    }

    /**
     * MISSING FUNCTION: Apply advanced market adjustments
     */
    applyAdvancedMarketAdjustments(targetProperty, baseValue, comparables, method) {
        const adjustments = [];
        let totalAdjustment = 0;
        const borough = this.getBoroughFromNeighborhood(targetProperty.neighborhood);
        
        console.log(`       🔧 Applying adjustments for ${borough}...`);
        
        // STEP 1: Amenity adjustments
        const targetAmenities = this.extractAmenitiesFromListing(targetProperty);
        const amenityAdjustment = this.calculateAdvancedAmenityAdjustments(targetAmenities, baseValue.baseValue, borough);
        
        if (Math.abs(amenityAdjustment) > 25) {
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
            if (Math.abs(sqftAdjustment) > 50) {
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
        
        return {
            finalValue: finalValue,
            totalAdjustments: totalAdjustment,
            adjustments: adjustments,
            baseValue: baseValue.baseValue
        };
    }

    /**
     * MISSING FUNCTION: Calculate advanced amenity adjustments
     */
    calculateAdvancedAmenityAdjustments(amenities, baseRent, borough) {
        const boroughAdjustments = this.AMENITY_ADJUSTMENTS[borough] || this.AMENITY_ADJUSTMENTS.manhattan;
        let totalAdjustment = 0;
        
        for (const amenity of amenities) {
            const adjustmentValue = boroughAdjustments[amenity.toLowerCase().replace(/\s+/g, '_')];
            if (adjustmentValue) {
                totalAdjustment += adjustmentValue;
            }
        }
        
        return totalAdjustment;
    }

    /**
     * MISSING FUNCTION: Calculate advanced square footage adjustment
     */
    calculateAdvancedSquareFootageAdjustment(targetProperty, comparables) {
        const targetSqft = targetProperty.sqft || this.estimateSquareFootage(targetProperty);
        
        // Calculate average sqft of comparables
        const comparableSqft = comparables
            .filter(comp => comp.sqft > 0)
            .map(comp => comp.sqft);
            
        if (comparableSqft.length === 0) return 0;
        
        const avgComparableSqft = comparableSqft.reduce((a, b) => a + b, 0) / comparableSqft.length;
        const sqftDifference = targetSqft - avgComparableSqft;
        
        // Adjust roughly $2-4 per sqft difference depending on borough
        const borough = this.getBoroughFromNeighborhood(targetProperty.neighborhood);
        const adjustmentRate = {
            manhattan: 4,
            brooklyn: 3,
            queens: 2,
            bronx: 2
        }[borough] || 3;
        
        return Math.round(sqftDifference * adjustmentRate);
    }

    /**
     * MISSING FUNCTION: Calculate building quality adjustment
     */
    calculateBuildingQualityAdjustment(targetProperty, comparables) {
        // Simplified building quality assessment
        let qualityScore = 0;
        
        // Age factor
        if (targetProperty.yearBuilt) {
            if (targetProperty.yearBuilt >= 2010) qualityScore += 100;
            else if (targetProperty.yearBuilt >= 2000) qualityScore += 50;
            else if (targetProperty.yearBuilt >= 1990) qualityScore += 0;
            else qualityScore -= 50;
        }
        
        // Building amenities factor
        const buildingAmenities = targetProperty.buildingAmenities || [];
        if (buildingAmenities.includes('doorman')) qualityScore += 100;
        if (buildingAmenities.includes('elevator')) qualityScore += 50;
        if (buildingAmenities.includes('gym')) qualityScore += 75;
        
        return qualityScore;
    }

    /**
     * MISSING FUNCTION: Calculate bathroom adjustment
     */
    calculateBathroomAdjustment(bathDifference, borough) {
        const adjustmentRates = {
            manhattan: 200,
            brooklyn: 150,
            queens: 100,
            bronx: 75
        };
        
        const rate = adjustmentRates[borough] || 150;
        return Math.round(bathDifference * rate);
    }

    /**
     * MISSING FUNCTION: Calculate advanced confidence score
     */
    calculateAdvancedConfidenceScore(targetProperty, comparables, method) {
        let confidence = 0;
        
        // Base confidence by method
        const methodConfidence = {
            'exact_bed_bath_amenity_match': 90,
            'bed_bath_specific_pricing': 80,
            'bed_specific_with_adjustments': 70,
            'price_per_sqft_fallback': 60
        };
        
        confidence += methodConfidence[method] || 50;
        
        // Adjust for sample size
        const sampleSize = comparables.length;
        if (sampleSize >= 20) confidence += 10;
        else if (sampleSize >= 10) confidence += 5;
        else if (sampleSize < 5) confidence -= 10;
        
        // Adjust for data quality
        const dataQuality = comparables.filter(comp => this.hasReasonableDataQuality(comp)).length / comparables.length;
        confidence += Math.round((dataQuality - 0.5) * 20);
        
        return Math.max(0, Math.min(100, confidence));
    }

    /**
     * MISSING FUNCTION: Extract amenities from listing
     */
    extractAmenitiesFromListing(listing) {
        const amenities = [];
        const description = (listing.description || '').toLowerCase();
        const listingAmenities = listing.amenities || [];
        const buildingAmenities = listing.buildingAmenities || [];
        
        // Combine all amenity sources
        const allAmenities = [...listingAmenities, ...buildingAmenities];
        
        // Standard amenity keywords
        const amenityKeywords = {
            'elevator': ['elevator', 'lift'],
            'doorman': ['doorman', 'concierge', 'front desk'],
            'gym': ['gym', 'fitness', 'workout'],
            'roof_deck': ['roof deck', 'rooftop', 'roof terrace'],
            'laundry': ['laundry', 'washer', 'dryer'],
            'parking': ['parking', 'garage'],
            'balcony': ['balcony', 'terrace', 'patio'],
            'dishwasher': ['dishwasher'],
            'air_conditioning': ['air conditioning', 'a/c', 'ac'],
            'hardwood': ['hardwood', 'wood floors']
        };
        
        // Check description and amenity lists
        for (const [amenity, keywords] of Object.entries(amenityKeywords)) {
            const hasAmenity = keywords.some(keyword => 
                description.includes(keyword) || 
                allAmenities.some(a => a.toLowerCase().includes(keyword))
            );
            
            if (hasAmenity) {
                amenities.push(amenity);
            }
        }
        
        return amenities;
    }

    /**
     * MISSING FUNCTION: Check if listing has reasonable data quality
     */
    hasReasonableDataQuality(listing) {
        // Must have valid price
        if (!listing.price || listing.price <= 0) return false;
        
        // Must have valid bedroom/bathroom count
        if ((listing.bedrooms || 0) < 0 || (listing.bathrooms || 0) < 0) return false;
        
        // Must have some description or address
        if (!listing.description && !listing.address) return false;
        
        // Price reasonableness check (NYC rental market)
        if (listing.price < 1000 || listing.price > 20000) return false;
        
        return true;
    }

    /**
     * MISSING FUNCTION: Estimate square footage
     */
    estimateSquareFootage(listing) {
        const bedrooms = listing.bedrooms || 0;
        const bathrooms = listing.bathrooms || 1;
        
        // NYC apartment size estimates
        let baseSqft = 0;
        
        if (bedrooms === 0) baseSqft = 400; // Studio
        else if (bedrooms === 1) baseSqft = 650;
        else if (bedrooms === 2) baseSqft = 950;
        else if (bedrooms === 3) baseSqft = 1200;
        else baseSqft = 1200 + (bedrooms - 3) * 200;
        
        // Add bathroom space
        baseSqft += (bathrooms - 1) * 50;
        
        return baseSqft;
    }

    /**
     * MISSING FUNCTION: Calculate median
     */
    calculateMedian(values) {
        if (values.length === 0) return 0;
        
        const sorted = [...values].sort((a, b) => a - b);
        const middle = Math.floor(sorted.length / 2);
        
        if (sorted.length % 2 === 0) {
            return (sorted[middle - 1] + sorted[middle]) / 2;
        } else {
            return sorted[middle];
        }
    }

    /**
     * MISSING FUNCTION: Generate advanced valuation reasoning
     */
    generateAdvancedValuationReasoning(targetProperty, baseValue, adjustedValue, valuationResult) {
        const steps = [];
        const confidenceFactors = {};
        
        steps.push(`Used ${valuationResult.method} with ${valuationResult.comparables.length} comparable properties`);
        steps.push(`Base market rent: ${baseValue.baseValue.toLocaleString()} (${baseValue.method})`);
        
        if (adjustedValue.adjustments.length > 0) {
            steps.push(`Applied ${adjustedValue.adjustments.length} adjustments totaling ${adjustedValue.totalAdjustments.toLocaleString()}`);
            for (const adj of adjustedValue.adjustments) {
                steps.push(`  - ${adj.description}: ${adj.amount >= 0 ? '+' : ''}${adj.amount.toLocaleString()}`);
            }
        }
        
        steps.push(`Final market estimate: ${adjustedValue.finalValue.toLocaleString()}`);
        
        confidenceFactors.method_reliability = valuationResult.method;
        confidenceFactors.sample_size = valuationResult.comparables.length;
        confidenceFactors.data_quality = 'good';
        
        return {
            steps,
            confidenceFactors
        };
    }

    /**
     * MISSING FUNCTION: Save all results to database
     */
    async saveAllResults(analyzedListings) {
        console.log(`\n💾 Saving ${analyzedListings.length} rent-stabilized listings to database...`);
        
        if (analyzedListings.length === 0) {
            console.log('   ⚠️ No listings to save');
            return;
        }
        
        try {
            const dbRecords = analyzedListings.map(listing => ({
                listing_id: listing.id,
                listing_url: listing.url,
                address: listing.address,
                neighborhood: listing.neighborhood,
                borough: this.getBoroughFromNeighborhood(listing.neighborhood),
                zip_code: null, // Extract from address if needed
                monthly_rent: listing.price,
                estimated_market_rent: listing.estimatedMarketRent,
                undervaluation_percent: listing.undervaluationPercent,
                potential_monthly_savings: listing.potentialSavings,
                bedrooms: listing.bedrooms,
                bathrooms: listing.bathrooms,
                sqft: listing.sqft,
                description: listing.description,
                amenities: listing.amenities || [],
                building_amenities: listing.buildingAmenities || [],
                building_type: listing.buildingType,
                year_built: listing.yearBuilt,
                total_units_in_building: listing.totalUnits,
                broker_fee: listing.brokerFee,
                available_date: listing.availableDate,
                lease_term: listing.leaseTerms,
                pet_policy: listing.petPolicy,
                broker_name: listing.brokerName,
                broker_phone: listing.brokerPhone,
                broker_email: null,
                listing_agent: listing.brokerName,
                street_easy_score: null,
                walk_score: null,
                transit_score: null,
                images: listing.images || [],
                virtual_tour_url: listing.virtualTourUrl,
                floor_plan_url: null,
                rent_stabilized_confidence: listing.rentStabilizedConfidence,
                rent_stabilized_method: listing.rentStabilizedMethod,
                rent_stabilization_analysis: listing.rentStabilizationAnalysis || {},
                undervaluation_method: listing.undervaluationMethod,
                undervaluation_confidence: listing.undervaluationConfidence,
                comparables_used: listing.comparablesUsed,
                undervaluation_analysis: listing.undervaluationAnalysis || {},
                deal_quality_score: this.calculateDealQualityScore(listing),
                ranking_in_neighborhood: null, // Will be calculated later
                neighborhood_median_rent: null, // Will be calculated later
                comparable_properties_in_area: listing.comparablesUsed,
                risk_factors: this.calculateRiskFactors(listing),
                opportunity_score: this.calculateOpportunityScore(listing),
                market_classification: listing.marketClassification,
                analysis_date: new Date().toISOString(),
                discovered_at: new Date().toISOString(),
                analyzed_at: new Date().toISOString(),
                last_verified: new Date().toISOString(),
                display_status: 'active',
                tags: this.generateTags(listing),
                admin_notes: null
            }));

            // Insert records using upsert to handle duplicates
            const { data, error } = await this.supabase
                .from('undervalued_rent_stabilized')
                .upsert(dbRecords, { 
                    onConflict: 'listing_id',
                    ignoreDuplicates: false 
                });

            if (error) {
                console.error('   ❌ Database save error:', error.message);
                throw error;
            }

            console.log(`   ✅ Successfully saved ${analyzedListings.length} listings to undervalued_rent_stabilized table`);
            
            // Update rankings and neighborhood stats
            await this.updateNeighborhoodRankings();
            
        } catch (error) {
            console.error('   ❌ Failed to save results:', error.message);
            throw error;
        }
    }

    /**
     * MISSING FUNCTION: Generate comprehensive report
     */
    generateComprehensiveReport(analyzedListings) {
        console.log('\n' + '='.repeat(80));
        console.log('🏆 NYC RENT-STABILIZED APARTMENT ANALYSIS REPORT');
        console.log('='.repeat(80));
        
        // Overall summary
        const totalListings = analyzedListings.length;
        const undervaluedCount = analyzedListings.filter(l => l.marketClassification === 'undervalued').length;
        const moderatelyUndervaluedCount = analyzedListings.filter(l => l.marketClassification === 'moderately_undervalued').length;
        const marketRateCount = analyzedListings.filter(l => l.marketClassification === 'market_rate').length;
        
        console.log(`📊 SUMMARY:`);
        console.log(`   Total rent-stabilized listings found: ${totalListings}`);
        console.log(`   Significantly undervalued (15%+ below market): ${undervaluedCount}`);
        console.log(`   Moderately undervalued (5-15% below market): ${moderatelyUndervaluedCount}`);
        console.log(`   Market rate (within 5% of market): ${marketRateCount}`);
        
        // Top deals
        const topDeals = analyzedListings
            .filter(l => l.undervaluationPercent > 0)
            .sort((a, b) => b.undervaluationPercent - a.undervaluationPercent)
            .slice(0, 10);
            
        if (topDeals.length > 0) {
            console.log(`\n🎯 TOP ${Math.min(10, topDeals.length)} UNDERVALUED DEALS:`);
            topDeals.forEach((listing, index) => {
                console.log(`   ${index + 1}. ${listing.address} - ${listing.neighborhood}`);
                console.log(`      💰 Rent: ${listing.price.toLocaleString()} | Market: ${listing.estimatedMarketRent.toLocaleString()}`);
                console.log(`      📊 ${listing.undervaluationPercent.toFixed(1)}% below market | Saves ${listing.potentialSavings.toLocaleString()}/month`);
                console.log(`      🏠 ${listing.bedrooms}BR/${listing.bathrooms}BA | ${listing.rentStabilizedConfidence}% RS confidence`);
            });
        }
        
        // Neighborhood breakdown
        const neighborhoodStats = {};
        analyzedListings.forEach(listing => {
            if (!neighborhoodStats[listing.neighborhood]) {
                neighborhoodStats[listing.neighborhood] = {
                    total: 0,
                    undervalued: 0,
                    avgSavings: 0,
                    totalSavings: 0
                };
            }
            neighborhoodStats[listing.neighborhood].total++;
            if (listing.marketClassification === 'undervalued') {
                neighborhoodStats[listing.neighborhood].undervalued++;
            }
            neighborhoodStats[listing.neighborhood].totalSavings += listing.potentialSavings;
        });
        
        // Calculate averages
        Object.keys(neighborhoodStats).forEach(neighborhood => {
            const stats = neighborhoodStats[neighborhood];
            stats.avgSavings = stats.total > 0 ? stats.totalSavings / stats.total : 0;
        });
        
        console.log(`\n🗽 NEIGHBORHOOD BREAKDOWN:`);
        Object.entries(neighborhoodStats)
            .sort((a, b) => b[1].undervalued - a[1].undervalued)
            .forEach(([neighborhood, stats]) => {
                console.log(`   ${neighborhood}: ${stats.total} total, ${stats.undervalued} undervalued (avg savings: ${Math.round(stats.avgSavings).toLocaleString()}/mo)`);
            });
        
        console.log('\n📋 NEXT STEPS:');
        console.log('   1. Check your Supabase "undervalued_rent_stabilized" table for all results');
        console.log('   2. Filter by market_classification for different strategies:');
        console.log('      - "undervalued": Priority targets (15%+ below market)');
        console.log('      - "moderately_undervalued": Good opportunities (5-15% below market)');
        console.log('      - "market_rate": Fair market deals');
        console.log('      - "overvalued": Above market (may have other benefits)');
        console.log('   3. Verify rent stabilization status with landlord/DHCR before applying');
        console.log('   4. Contact listings immediately - good deals move fast in NYC');
    }

    /**
     * MISSING FUNCTION: Get borough from neighborhood
     */
    getBoroughFromNeighborhood(neighborhood) {
        for (const [borough, neighborhoods] of Object.entries(this.BOROUGH_MAP)) {
            if (neighborhoods.includes(neighborhood)) {
                return borough;
            }
        }
        return 'manhattan'; // Default fallback
    }

    /**
     * Calculate deal quality score (0-100)
     */
    calculateDealQualityScore(listing) {
        let score = 50; // Base score
        
        // Undervaluation bonus
        if (listing.undervaluationPercent >= 20) score += 30;
        else if (listing.undervaluationPercent >= 15) score += 25;
        else if (listing.undervaluationPercent >= 10) score += 20;
        else if (listing.undervaluationPercent >= 5) score += 15;
        
        // Rent stabilization confidence bonus
        score += Math.round(listing.rentStabilizedConfidence * 0.2);
        
        // Undervaluation confidence bonus
        score += Math.round(listing.undervaluationConfidence * 0.1);
        
        // Amenity bonus
        const amenityCount = (listing.amenities || []).length + (listing.buildingAmenities || []).length;
        score += Math.min(15, amenityCount * 2);
        
        return Math.max(0, Math.min(100, score));
    }

    /**
     * Calculate risk factors
     */
    calculateRiskFactors(listing) {
        const risks = [];
        
        if (listing.rentStabilizedConfidence < 70) {
            risks.push('Low rent stabilization confidence');
        }
        
        if (listing.undervaluationConfidence < 60) {
            risks.push('Low valuation confidence');
        }
        
        if (listing.comparablesUsed < 10) {
            risks.push('Limited comparable data');
        }
        
        if (listing.brokerFee && listing.brokerFee.toLowerCase().includes('fee')) {
            risks.push('Broker fee required');
        }
        
        return risks;
    }

    /**
     * Calculate opportunity score (0-100)
     */
    calculateOpportunityScore(listing) {
        let score = 0;
        
        // Base score from undervaluation
        score += Math.min(50, listing.undervaluationPercent * 2);
        
        // Monthly savings factor
        if (listing.potentialSavings >= 1000) score += 20;
        else if (listing.potentialSavings >= 500) score += 15;
        else if (listing.potentialSavings >= 250) score += 10;
        
        // Rent stabilization certainty
        score += Math.round(listing.rentStabilizedConfidence * 0.3);
        
        return Math.max(0, Math.min(100, score));
    }

    /**
     * Generate tags for listing
     */
    generateTags(listing) {
        const tags = [];
        
        if (listing.marketClassification === 'undervalued') {
            tags.push('highly_undervalued');
        }
        
        if (listing.rentStabilizedConfidence >= 80) {
            tags.push('high_rs_confidence');
        }
        
        if (listing.potentialSavings >= 500) {
            tags.push('high_savings');
        }
        
        const borough = this.getBoroughFromNeighborhood(listing.neighborhood);
        tags.push(`${borough}_listing`);
        
        if ((listing.amenities || []).length + (listing.buildingAmenities || []).length >= 5) {
            tags.push('amenity_rich');
        }
        
        return tags;
    }

    /**
     * Update neighborhood rankings
     */
    async updateNeighborhoodRankings() {
        try {
            // This would calculate rankings within each neighborhood
            // For now, we'll skip this complex calculation
            console.log('   📊 Neighborhood rankings calculation skipped (implement if needed)');
        } catch (error) {
            console.error('   ⚠️ Failed to update rankings:', error.message);
        }
    }
}

// Export for use in other modules
module.exports = RentStabilizedUndervaluedDetector;

/**
 * MAIN EXECUTION FUNCTION
 */
async function main() {
    console.log('🚀 Starting NYC Rent-Stabilized Apartment Finder...\n');
    
    const detector = new RentStabilizedUndervaluedDetector();
    
    try {
        // Check for command line arguments
        const args = process.argv.slice(2);
        const isTestMode = args.includes('--test') || process.env.TEST_NEIGHBORHOOD;
        const isSetupMode = args.includes('--setup');
        
        if (isSetupMode) {
            console.log('🛠️ Setup mode - would initialize database tables and download DHCR data');
            console.log('   (Setup functionality can be implemented as needed)');
            return;
        }
        
        // Determine neighborhoods to analyze
        let neighborhoods = [];
        if (detector.testNeighborhood) {
            neighborhoods = [detector.testNeighborhood];
            console.log(`🧪 Test mode: analyzing only ${detector.testNeighborhood}\n`);
        } else if (isTestMode) {
            neighborhoods = ['east-village', 'lower-east-side']; // Limited test
            console.log('🧪 Test mode: analyzing limited neighborhoods\n');
        } else {
            // Full analysis - will use default neighborhoods
            console.log('🏙️ Full analysis mode\n');
        }
        
        const results = await detector.findUndervaluedRentStabilizedListings({
            neighborhoods: neighborhoods.length > 0 ? neighborhoods : undefined,
            maxListingsPerNeighborhood: detector.maxListingsPerNeighborhood,
            testMode: isTestMode
        });
        
        console.log('\n🎉 Analysis complete!');
        console.log(`📊 Scanned ${results.totalListingsScanned} total listings`);
        console.log(`🏠 Found ${results.rentStabilizedFound} rent-stabilized listings`);
        console.log(`💾 Saved ${results.allRentStabilizedSaved} listings to database`);
        console.log(`🎯 Check your Supabase 'undervalued_rent_stabilized' table for all results`);
        
        // Show top deals
        const topDeals = results.results
            .filter(r => r.undervaluationPercent > 15)
            .slice(0, 5);
            
        if (topDeals.length > 0) {
            console.log(`\n🏆 Top ${topDeals.length} undervalued deals:`);
            topDeals.forEach((deal, index) => {
                console.log(`   ${index + 1}. ${deal.address} - ${deal.undervaluationPercent.toFixed(1)}% below market`);
            });
        }
        
        return results;
        
    } catch (error) {
        console.error('\n💥 Analysis failed:', error.message);
        console.error('Stack trace:', error.stack);
        process.exit(1);
    }
}

// Run if executed directly
if (require.main === module) {
    main().catch(error => {
        console.error('💥 Rent-stabilized detector crashed:', error.message);
        console.error('Stack trace:', error.stack);
        process.exit(1);
    });
}
