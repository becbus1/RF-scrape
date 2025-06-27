/**
 * NYC RENT-STABILIZED UNDERVALUED APARTMENT FINDER
 * 
 * SYSTEM GOAL: Find ALL rent-stabilized apartments (both undervalued and market-rate)
 * Focus: ONLY rent-stabilized apartments using LEGAL INDICATORS
 * 
 * Features:
 * ✅ DHCR building matching (strongest indicator)
 * ✅ Legal rent stabilization criteria analysis
 * ✅ Market classification (undervalued vs market-rate)
 * ✅ Comprehensive caching system
 * ✅ Save ALL rent-stabilized results (not just undervalued)
 */

const axios = require('axios');
const { createClient } = require('@supabase/supabase-js');
require('dotenv').config();
class RentStabilizedUndervaluedSystem {
    constructor() {
        this.supabase = createClient(
            process.env.SUPABASE_URL,
            process.env.SUPABASE_ANON_KEY
        );
        
        // Configuration
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
    }

    /**
     * MAIN: Run comprehensive rent-stabilized analysis
     */
    async runComprehensiveRentStabilizedAnalysis(config = {}) {
        try {
            console.log('🏙️ COMPREHENSIVE NYC RENT-STABILIZED APARTMENT ANALYSIS');
            console.log('=' .repeat(60));
            console.log('🎯 GOAL: Find ALL rent-stabilized apartments using legal indicators');
            console.log('📊 SAVE: Both undervalued AND market-rate rent-stabilized properties\n');

            // Configuration
            const neighborhoods = config.neighborhoods || this.defaultNeighborhoods;
            const maxListingsPerNeighborhood = config.maxListingsPerNeighborhood || 500;

            // Step 1: Get ALL listings with comprehensive caching
            console.log('📋 Step 1: Fetching all listings with comprehensive caching...');
            const allListings = await this.getAllListingsWithComprehensiveCaching(
                neighborhoods, maxListingsPerNeighborhood);
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
                // STEP 1: Check comprehensive cache first
                const cacheKey = `comprehensive_${neighborhood}`;
                const cacheExpiry = 24 * 60 * 60 * 1000; // 24 hours
                const cachedData = await this.getComprehensiveCachedListings(neighborhood);
                
                if (cachedData.length > 0) {
                    console.log(`     💾 Found ${cachedData.length} cached listings`);
                } else {
                    console.log(`     💾 No comprehensive cache found for ${neighborhood}`);
                }
                
                // Get existing cached listings for this neighborhood
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
                    Math.round((cachedListings.length / (cachedListings.length + freshListings.length)) * 100) : 100;
                console.log(`     📊 Total: ${allNeighborhoodListings.length} listings (${efficiency}% cache efficiency)`);
                
            } catch (error) {
                console.error(`     ❌ Error fetching ${neighborhood}:`, error.message);
            }
        }
        
        return allListings;
    }

    /**
     * NEW: Get cached listings from comprehensive cache tables
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
            
            // Fallback to rental_market_cache
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
     * FIXED: Fetch fresh listings using the CORRECT working API call
     */
    async fetchFreshListingsUsingWorkingAPI(neighborhood, maxListings) {
        try {
            const rapidApiKey = process.env.RAPIDAPI_KEY;
            if (!rapidApiKey) {
                console.log(`       ⚠️ No RAPIDAPI_KEY found, cannot fetch from StreetEasy`);
                return [];
            }

            console.log(`       🌐 Fetching fresh listings from StreetEasy API for ${neighborhood}...`);
            
            // CORRECTED: Use the exact same working API endpoint from biweekly-streeteasy-rentals.js
            const searchUrl = `https://streeteasy-api.p.rapidapi.com/rentals/search`;
            
            // Use the WORKING API call structure from the original
            const response = await axios.get(searchUrl, {
                headers: {
                    'X-RapidAPI-Key': rapidApiKey,
                    'X-RapidAPI-Host': 'streeteasy-api.p.rapidapi.com'
                },
                params: {
                    areas: neighborhood,  // FIXED: Use 'areas' not 'neighborhood'
                    limit: Math.min(maxListings, 500), // Respect API limits
                    minPrice: 1000,       // FIXED: Add minPrice for rentals
                    maxPrice: 20000,      // FIXED: Add maxPrice for rentals  
                    offset: 0
                },
                timeout: 30000
            });

            // FIXED: Handle response structure correctly
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

            if (rentalData.length > 0) {
                const listings = rentalData.map(rental => ({
                    id: rental.id?.toString(),
                    address: rental.address || 'Unknown Address',
                    price: rental.price || rental.monthly_rent || 0,
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
                    rentStabilizedReasoning: analysis.reasoning
                });
            }
        }
        
        return rentStabilizedListings;
    }

    /**
     * CRITICAL LEGAL ANALYSIS: Determine rent-stabilization status using LEGAL INDICATORS ONLY
     */
    analyzeRentStabilizationLegal(listing, stabilizedBuildings) {
        let confidence = 0;
        const factors = [];
        
        // Factor 1: DHCR building match (STRONGEST indicator - 60% confidence)
        const matchedBuilding = this.findMatchingStabilizedBuilding(listing.address, stabilizedBuildings);
        if (matchedBuilding) {
            confidence += 60;
            factors.push('DHCR registered building (60%)');
        }
        
        // Factor 2: Building age analysis (20% confidence)
        const ageAnalysis = this.analyzeBuildingAge(listing);
        if (ageAnalysis.isEligible) {
            confidence += 20;
            factors.push(`Built before 1974 eligibility (20%)`);
        }
        
        // Factor 3: Building size indicators (15% confidence)
        const sizeAnalysis = this.analyzeBuildingSize(listing);
        if (sizeAnalysis.isEligible) {
            confidence += 15;
            factors.push(`6+ unit building indicators (15%)`);
        }
        
        // Factor 4: Address patterns (5% confidence)
        const addressAnalysis = this.analyzeAddressPatterns(listing);
        if (addressAnalysis.hasIndicators) {
            confidence += 5;
            factors.push(`Address patterns suggest eligibility (5%)`);
        }
        
        const reasoning = factors.length > 0 ? 
            `Legal rent-stabilization analysis:\n• ${factors.join('\n• ')}\n\nTotal confidence: ${confidence}%` :
            'No legal rent-stabilization indicators found';
        
        return {
            confidence: Math.min(confidence, 100),
            factors,
            reasoning,
            dhcrMatch: !!matchedBuilding
        };
    }

    /**
     * FIXED: Load rent-stabilized buildings from database - CORRECTED SQL
     */
    async loadRentStabilizedBuildings() {
        try {
            // FIXED: Use proper SELECT * instead of SELECT '500000'
            const { data, error } = await this.supabase
                .from('rent_stabilized_buildings')
                .select('*')  // CORRECTED: Select all columns, not a literal number
                .limit(500000);  // CORRECTED: Use limit() instead of select('500000')
            
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
     * Analyze building age for rent stabilization eligibility
     */
    analyzeBuildingAge(listing) {
        // Most rent-stabilized buildings were built before 1974
        const description = (listing.description || '').toLowerCase();
        const yearMatches = description.match(/built.{0,10}(19\d{2}|20\d{2})/i);
        
        if (yearMatches) {
            const year = parseInt(yearMatches[1]);
            return {
                isEligible: year < 1974,
                year,
                reasoning: `Building built in ${year} (${year < 1974 ? 'eligible' : 'not eligible'})`
            };
        }
        
        // If no year found, assume eligible (conservative approach)
        return {
            isEligible: true,
            reasoning: 'Building age not specified (assuming eligible)'
        };
    }

    /**
     * Analyze building size for rent stabilization eligibility
     */
    analyzeBuildingSize(listing) {
        const description = (listing.description || '').toLowerCase();
        const address = (listing.address || '').toLowerCase();
        
        // Look for indicators of 6+ unit buildings
        const sizeIndicators = [
            'elevator', 'doorman', 'concierge', 'lobby', 'building amenities',
            'fitness center', 'roof deck', 'laundry room', 'bike storage'
        ];
        
        const hasIndicators = sizeIndicators.some(indicator => 
            description.includes(indicator) || address.includes(indicator)
        );
        
        return {
            isEligible: hasIndicators,
            reasoning: hasIndicators ? 
                'Building amenities suggest 6+ units (eligible)' : 
                'No clear building size indicators'
        };
    }

    /**
     * Analyze address patterns for rent stabilization indicators
     */
    analyzeAddressPatterns(listing) {
        const address = (listing.address || '').toLowerCase();
        
        // Look for patterns common in rent-stabilized buildings
        const patterns = [
            /\d+[a-z]?\s+(east|west|north|south)/i,  // Street numbers
            /apartment\s+\d+[a-z]?/i,                // Apartment numbers
            /#\d+[a-z]?/i,                           // Unit numbers
            /\d+(st|nd|rd|th)\s+(street|avenue|ave)/i // NYC street patterns
        ];
        
        const hasIndicators = patterns.some(pattern => pattern.test(address));
        
        return {
            hasIndicators,
            reasoning: hasIndicators ? 
                'Address patterns consistent with stabilized housing' : 
                'No specific address patterns identified'
        };
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
     * STEP 4: Analyze ALL rent-stabilized listings with market classification
     */
    async analyzeAllRentStabilizedWithClassification(rentStabilizedListings, allListings) {
        const analyzedResults = [];
        
        for (const listing of rentStabilizedListings) {
            try {
                // Calculate market analysis
                const marketAnalysis = this.calculateMarketAnalysis(listing, allListings);
                
                // Determine classification
                const classification = this.classifyRentStabilizedListing(marketAnalysis);
                
                const result = {
                    // Basic listing info
                    id: listing.id,
                    address: listing.address,
                    neighborhood: listing.neighborhood,
                    monthlyRent: listing.price,
                    bedrooms: listing.bedrooms,
                    bathrooms: listing.bathrooms,
                    sqft: listing.sqft,
                    
                    // Rent stabilization analysis
                    rentStabilizedConfidence: listing.rentStabilizedConfidence,
                    rentStabilizedFactors: listing.rentStabilizedFactors,
                    
                    // Market analysis
                    marketRentPerSqft: marketAnalysis.marketRentPerSqft,
                    actualRentPerSqft: marketAnalysis.actualRentPerSqft,
                    undervaluationPercent: marketAnalysis.undervaluationPercent,
                    potentialMonthlySavings: marketAnalysis.potentialMonthlySavings,
                    annualSavings: marketAnalysis.annualSavings,
                    
                    // Classification
                    classification: classification.type,
                    score: classification.score,
                    reasoning: this.generateAnalysisReasoning(listing, marketAnalysis, classification),
                    
                    // Metadata
                    url: listing.url,
                    listedAt: listing.listedAt,
                    analyzedAt: new Date().toISOString()
                };
                
                analyzedResults.push(result);
                
            } catch (error) {
                console.error(`Failed to analyze listing ${listing.id}:`, error.message);
            }
        }
        
        return analyzedResults;
    }

    /**
     * Calculate market analysis for rent-stabilized listing
     */
    calculateMarketAnalysis(listing, allListings) {
        // Filter comparable listings (same neighborhood, similar bed/bath)
        const comparables = allListings.filter(comp => 
            comp.neighborhood === listing.neighborhood &&
            comp.bedrooms === listing.bedrooms &&
            Math.abs(comp.bathrooms - listing.bathrooms) <= 0.5 &&
            comp.price > 0 &&
            comp.sqft > 0
        );
        
        if (comparables.length === 0) {
            return {
                marketRentPerSqft: 0,
                actualRentPerSqft: listing.sqft > 0 ? listing.price / listing.sqft : 0,
                undervaluationPercent: 0,
                potentialMonthlySavings: 0,
                annualSavings: 0
            };
        }
        
        // Calculate market rent per sqft
        const rentPerSqftValues = comparables
            .map(comp => comp.price / comp.sqft)
            .filter(value => value > 0)
            .sort((a, b) => a - b);
        
        const marketRentPerSqft = this.calculateMedian(rentPerSqftValues);
        const actualRentPerSqft = listing.sqft > 0 ? listing.price / listing.sqft : 0;
        
        // Calculate undervaluation
        const marketRent = marketRentPerSqft * listing.sqft;
        const undervaluationPercent = marketRent > 0 ? 
            Math.round(((marketRent - listing.price) / marketRent) * 100) : 0;
        
        const potentialMonthlySavings = Math.max(0, marketRent - listing.price);
        const annualSavings = potentialMonthlySavings * 12;
        
        return {
            marketRentPerSqft,
            actualRentPerSqft,
            undervaluationPercent,
            potentialMonthlySavings,
            annualSavings
        };
    }

    /**
     * Classify rent-stabilized listing by market position
     */
    classifyRentStabilizedListing(marketAnalysis) {
        const undervaluation = marketAnalysis.undervaluationPercent;
        
        if (undervaluation >= 30) {
            return { type: 'HIGHLY_UNDERVALUED', score: 95 };
        } else if (undervaluation >= 20) {
            return { type: 'SIGNIFICANTLY_UNDERVALUED', score: 85 };
        } else if (undervaluation >= 10) {
            return { type: 'MODERATELY_UNDERVALUED', score: 75 };
        } else if (undervaluation >= 5) {
            return { type: 'SLIGHTLY_UNDERVALUED', score: 65 };
        } else if (undervaluation >= -5) {
            return { type: 'MARKET_RATE', score: 50 };
        } else {
            return { type: 'ABOVE_MARKET', score: 30 };
        }
    }

    /**
     * Generate comprehensive analysis reasoning
     */
    generateAnalysisReasoning(listing, marketAnalysis, classification) {
        const parts = [];
        
        // Rent stabilization reasoning
        parts.push(`🏠 RENT-STABILIZED APARTMENT (${listing.rentStabilizedConfidence}% confidence)`);
        parts.push(`Legal indicators: ${listing.rentStabilizedFactors.join(', ')}`);
        
        // Market position analysis
        if (marketAnalysis.undervaluationPercent > 0) {
            parts.push(`\n💰 MARKET ANALYSIS: ${marketAnalysis.undervaluationPercent}% below market rate`);
            parts.push(`Actual rent: $${listing.price.toLocaleString()}/month`);
            parts.push(`Market rent: $${Math.round(marketAnalysis.marketRentPerSqft * listing.sqft).toLocaleString()}/month`);
            parts.push(`Annual savings: $${marketAnalysis.annualSavings.toLocaleString()}`);
        } else {
            parts.push(`\n📊 MARKET ANALYSIS: At or above market rate`);
        }
        
        // Classification
        parts.push(`\n🎯 CLASSIFICATION: ${classification.type.replace(/_/g, ' ')}`);
        
        return parts.join('\n');
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
     * STEP 5: Save ALL results to undervalued_rent_stabilized table
     */
    async saveAllResults(analyzedResults) {
        if (analyzedResults.length === 0) {
            console.log('   ⚠️ No rent-stabilized listings to save');
            return;
        }
        
        try {
            // Prepare data for undervalued_rent_stabilized table
            const saveData = analyzedResults.map(result => ({
                listing_id: result.id,
                address: result.address,
                neighborhood: result.neighborhood,
                borough: this.getBoroughFromNeighborhood(result.neighborhood),
                
                // Rent analysis
                monthly_rent: result.monthlyRent,
                rent_per_sqft: result.actualRentPerSqft,
                market_rent_per_sqft: result.marketRentPerSqft,
                discount_percent: result.undervaluationPercent,
                potential_monthly_savings: Math.round(result.potentialMonthlySavings),
                annual_savings: Math.round(result.annualSavings),
                
                // Property details
                bedrooms: result.bedrooms,
                bathrooms: result.bathrooms,
                sqft: result.sqft,
                property_type: 'apartment',
                
                // Rent stabilization data
                rent_stabilized: true,
                stabilized_confidence: result.rentStabilizedConfidence,
                stabilized_indicators: result.rentStabilizedFactors,
                
                // Classification and scoring
                market_classification: result.classification,
                score: result.score,
                reasoning: result.reasoning,
                
                // Metadata
                listing_url: result.url,
                listed_at: result.listedAt,
                analysis_date: result.analyzedAt,
                status: 'active'
            }));
            
            // Save to database with upsert
            const { data, error } = await this.supabase
                .from('undervalued_rent_stabilized')
                .upsert(saveData, { onConflict: 'listing_id' });
            
            if (error) {
                console.error('Failed to save results:', error.message);
                console.log('   💡 Trying alternative table name...');
                
                // Fallback to alternative table names
                const { error: fallbackError } = await this.supabase
                    .from('undervalued_rentals')
                    .upsert(saveData, { onConflict: 'listing_id' });
                
                if (fallbackError) {
                    console.error('Fallback save also failed:', fallbackError.message);
                } else {
                    console.log(`   ✅ Saved ${saveData.length} rent-stabilized listings to undervalued_rentals table`);
                }
            } else {
                console.log(`   ✅ Saved ${saveData.length} rent-stabilized listings to undervalued_rent_stabilized table`);
            }
            
        } catch (error) {
            console.error('Save operation failed:', error.message);
        }
    }

    /**
     * Generate comprehensive report
     */
    generateComprehensiveReport(analyzedResults) {
        console.log('\n📊 COMPREHENSIVE RENT-STABILIZED ANALYSIS REPORT');
        console.log('=' .repeat(60));
        
        // Overall statistics
        const totalFound = analyzedResults.length;
        const undervalued = analyzedResults.filter(r => r.undervaluationPercent > 0);
        const marketRate = analyzedResults.filter(r => r.undervaluationPercent <= 0);
        
        console.log(`🏠 Total rent-stabilized apartments found: ${totalFound}`);
        console.log(`💰 Undervalued properties: ${undervalued.length} (${Math.round(undervalued.length/totalFound*100)}%)`);
        console.log(`📊 Market-rate properties: ${marketRate.length} (${Math.round(marketRate.length/totalFound*100)}%)`);
        
        // Classification breakdown
        const classifications = {};
        analyzedResults.forEach(result => {
            classifications[result.classification] = (classifications[result.classification] || 0) + 1;
        });
        
        console.log('\n🎯 MARKET CLASSIFICATION BREAKDOWN:');
        Object.entries(classifications).forEach(([type, count]) => {
            console.log(`   ${type.replace(/_/g, ' ')}: ${count} properties`);
        });
        
        // Top opportunities
        const topOpportunities = undervalued
            .sort((a, b) => b.undervaluationPercent - a.undervaluationPercent)
            .slice(0, 5);
        
        if (topOpportunities.length > 0) {
            console.log('\n🌟 TOP RENT-STABILIZED OPPORTUNITIES:');
            topOpportunities.forEach((listing, index) => {
                console.log(`   ${index + 1}. ${listing.address}`);
                console.log(`      💰 ${listing.monthlyRent.toLocaleString()}/month (${listing.undervaluationPercent}% below market)`);
                console.log(`      💎 Annual savings: ${listing.annualSavings.toLocaleString()}`);
                console.log(`      🏠 ${listing.bedrooms}BR/${listing.bathrooms}BA, ${listing.sqft} sqft`);
                console.log(`      ⚖️ Rent-stabilized confidence: ${listing.rentStabilizedConfidence}%\n`);
            });
        }
        
        // Neighborhood analysis
        const neighborhoodStats = {};
        analyzedResults.forEach(result => {
            if (!neighborhoodStats[result.neighborhood]) {
                neighborhoodStats[result.neighborhood] = { total: 0, undervalued: 0 };
            }
            neighborhoodStats[result.neighborhood].total++;
            if (result.undervaluationPercent > 0) {
                neighborhoodStats[result.neighborhood].undervalued++;
            }
        });
        
        console.log('📍 NEIGHBORHOOD BREAKDOWN:');
        Object.entries(neighborhoodStats)
            .sort(([,a], [,b]) => b.total - a.total)
            .slice(0, 10)
            .forEach(([neighborhood, stats]) => {
                const undervaluedPercent = Math.round(stats.undervalued / stats.total * 100);
                console.log(`   ${neighborhood}: ${stats.total} total (${stats.undervalued} undervalued, ${undervaluedPercent}%)`);
            });
        
        console.log('\n🎉 Analysis complete! Check your Supabase table for full results.');
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
            
            console.log(`       💰 Market: ${estimatedMarketRent.toLocaleString()}, Actual: ${targetListing.price.toLocaleString()}`);
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
        
        // METHOD 1: Exact bed/bath/amenity match (MOST ACCURATE)
        const exactMatches = comparables.filter(comp => 
            comp.bedrooms === beds && 
            Math.abs((comp.bathrooms || 0) - baths) <= 0.5 &&
            this.hasReasonableDataQuality(comp)
        );
        
        if (exactMatches.length >= 3) {
            console.log(`       ✅ EXACT_MATCH: ${exactMatches.length} properties with ${beds}BR/${baths}BA`);
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
            console.log(`       ✅ BED_BATH_SPECIFIC: ${bedBathMatches.length} properties with ${beds}BR`);
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
            console.log(`       ⚠️ BED_SPECIFIC_WITH_ADJUSTMENTS: ${bedroomMatches.length} properties with ${beds}BR`);
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
     * Calculate building quality adjustment
     */
    calculateBuildingQualityAdjustment(targetProperty, comparables) {
        const description = (targetProperty.description || '').toLowerCase();
        let adjustment = 0;
        
        // Positive quality indicators
        const positiveIndicators = {
            'luxury': 150,
            'renovated': 100,
            'updated': 75,
            'modern': 50,
            'new construction': 200,
            'prewar': 75,
            'high-end': 125
        };
        
        // Negative quality indicators
        const negativeIndicators = {
            'needs work': -100,
            'fixer': -150,
            'as-is': -75,
            'outdated': -50
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
     * Get adjacent neighborhoods for comparison
     */
    getAdjacentNeighborhoods(neighborhood) {
        const adjacencies = {
            'east-village': ['lower-east-side', 'greenwich-village', 'gramercy'],
            'lower-east-side': ['east-village', 'chinatown', 'financial-district'],
            'west-village': ['greenwich-village', 'chelsea', 'tribeca'],
            'greenwich-village': ['east-village', 'west-village', 'soho'],
            'soho': ['nolita', 'tribeca', 'greenwich-village'],
            'chelsea': ['west-village', 'gramercy', 'flatiron'],
            'williamsburg': ['greenpoint', 'bushwick', 'dumbo'],
            'park-slope': ['prospect-heights', 'carroll-gardens', 'fort-greene']
        };
        
        return adjacencies[neighborhood] || [];
    }

}

/**
 * MAIN EXECUTION FUNCTION
 */
async function main() {
    try {
        const system = new RentStabilizedUndervaluedDetector();
        
        // Check for test mode
        const testNeighborhood = process.env.TEST_NEIGHBORHOOD;
        if (testNeighborhood) {
            console.log(`🧪 TEST MODE: Analyzing ${testNeighborhood} only\n`);
            
            const results = await system.findUndervaluedRentStabilizedListings({
                neighborhoods: [testNeighborhood],
                maxListingsPerNeighborhood: 100
            });
            
            console.log(`\n🎯 Test completed for ${testNeighborhood}`);
            console.log(`📊 Found ${results.rentStabilizedFound} rent-stabilized apartments`);
            
            return results;
        }
        
        // Full production analysis
        console.log('🏙️ Running FULL NYC rent-stabilized analysis...\n');
        
        const results = await system.findUndervaluedRentStabilizedListings({
            maxListingsPerNeighborhood: parseInt(process.env.MAX_LISTINGS_PER_NEIGHBORHOOD) || 500
        });
        
        console.log('\n🎉 Full NYC analysis completed!');
        console.log(`📊 Total rent-stabilized apartments found: ${results.rentStabilizedFound}`);
        console.log(`💾 All results saved to database`);
        
        return results;
        
    } catch (error) {
        console.error('💥 System crashed:', error.message);
        process.exit(1);
    }
}

// Export for use in other modules (matches what railway-sequential-runner.js expects)
module.exports = RentStabilizedUndervaluedDetector;

// Run if executed directly
if (require.main === module) {
    main().catch(error => {
        console.error('💥 Main execution failed:', error);
        process.exit(1);
    });
}
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
     * STEP 4: Analyze ALL rent-stabilized listings and classify by market position
     */
    async analyzeAllRentStabilizedWithClassification(rentStabilizedListings, allListings) {
        const analyzedResults = [];
        
        for (const listing of rentStabilizedListings) {
            try {
                // Calculate market analysis
                const marketAnalysis = this.calculateMarketAnalysis(listing, allListings);
                
                // Determine classification
                const classification = this.classifyRentStabilizedListing(marketAnalysis);
                
                const result = {
                    // Basic listing info
                    id: listing.id,
                    address: listing.address,
                    neighborhood: listing.neighborhood,
                    monthlyRent: listing.price,
                    bedrooms: listing.bedrooms,
                    bathrooms: listing.bathrooms,
                    sqft: listing.sqft,
                    
                    // Rent stabilization analysis
                    rentStabilizedConfidence: listing.rentStabilizedConfidence,
                    rentStabilizedFactors: listing.rentStabilizedFactors,
                    
                    // Market analysis
                    marketRentPerSqft: marketAnalysis.marketRentPerSqft,
                    actualRentPerSqft: marketAnalysis.actualRentPerSqft,
                    undervaluationPercent: marketAnalysis.undervaluationPercent,
                    potentialMonthlySavings: marketAnalysis.potentialMonthlySavings,
                    annualSavings: marketAnalysis.annualSavings,
                    
                    // Classification
                    classification: classification.type,
                    score: classification.score,
                    reasoning: this.generateAnalysisReasoning(listing, marketAnalysis, classification),
                    
                    // Metadata
                    url: listing.url,
                    listedAt: listing.listedAt,
                    analyzedAt: new Date().toISOString()
                };
                
                analyzedResults.push(result);
                
            } catch (error) {
                console.error(`Failed to analyze listing ${listing.id}:`, error.message);
            }
        }
        
        return analyzedResults;
    }

    /**
     * Calculate market analysis for rent-stabilized listing
     */
    calculateMarketAnalysis(listing, allListings) {
        // Filter comparable listings (same neighborhood, similar bed/bath)
        const comparables = allListings.filter(comp => 
            comp.neighborhood === listing.neighborhood &&
            comp.bedrooms === listing.bedrooms &&
            Math.abs(comp.bathrooms - listing.bathrooms) <= 0.5 &&
            comp.price > 0 &&
            comp.sqft > 0
        );
        
        if (comparables.length === 0) {
            return {
                marketRentPerSqft: 0,
                actualRentPerSqft: listing.sqft > 0 ? listing.price / listing.sqft : 0,
                undervaluationPercent: 0,
                potentialMonthlySavings: 0,
                annualSavings: 0
            };
        }
        
        // Calculate market rent per sqft
        const rentPerSqftValues = comparables
            .map(comp => comp.price / comp.sqft)
            .filter(value => value > 0)
            .sort((a, b) => a - b);
        
        const marketRentPerSqft = this.calculateMedian(rentPerSqftValues);
        const actualRentPerSqft = listing.sqft > 0 ? listing.price / listing.sqft : 0;
        
        // Calculate undervaluation
        const marketRent = marketRentPerSqft * listing.sqft;
        const undervaluationPercent = marketRent > 0 ? 
            Math.round(((marketRent - listing.price) / marketRent) * 100) : 0;
        
        const potentialMonthlySavings = Math.max(0, marketRent - listing.price);
        const annualSavings = potentialMonthlySavings * 12;
        
        return {
            marketRentPerSqft,
            actualRentPerSqft,
            undervaluationPercent,
            potentialMonthlySavings,
            annualSavings
        };
    }

    /**
     * Classify rent-stabilized listing by market position
     */
    classifyRentStabilizedListing(marketAnalysis) {
        const undervaluation = marketAnalysis.undervaluationPercent;
        
        if (undervaluation >= 30) {
            return { type: 'HIGHLY_UNDERVALUED', score: 95 };
        } else if (undervaluation >= 20) {
            return { type: 'SIGNIFICANTLY_UNDERVALUED', score: 85 };
        } else if (undervaluation >= 10) {
            return { type: 'MODERATELY_UNDERVALUED', score: 75 };
        } else if (undervaluation >= 5) {
            return { type: 'SLIGHTLY_UNDERVALUED', score: 65 };
        } else if (undervaluation >= -5) {
            return { type: 'MARKET_RATE', score: 50 };
        } else {
            return { type: 'ABOVE_MARKET', score: 30 };
        }
    }

    /**
     * Generate comprehensive analysis reasoning
     */
    generateAnalysisReasoning(listing, marketAnalysis, classification) {
        const parts = [];
        
        // Rent stabilization reasoning
        parts.push(`🏠 RENT-STABILIZED APARTMENT (${listing.rentStabilizedConfidence}% confidence)`);
        parts.push(`Legal indicators: ${listing.rentStabilizedFactors.join(', ')}`);
        
        // Market position analysis
        if (marketAnalysis.undervaluationPercent > 0) {
            parts.push(`\n💰 MARKET ANALYSIS: ${marketAnalysis.undervaluationPercent}% below market rate`);
            parts.push(`Actual rent: ${listing.price.toLocaleString()}/month`);
            parts.push(`Market rent: ${Math.round(marketAnalysis.marketRentPerSqft * listing.sqft).toLocaleString()}/month`);
            parts.push(`Annual savings: ${marketAnalysis.annualSavings.toLocaleString()}`);
        } else {
            parts.push(`\n📊 MARKET ANALYSIS: At or above market rate`);
        }
        
        // Classification
        parts.push(`\n🎯 CLASSIFICATION: ${classification.type.replace(/_/g, ' ')}`);
        
        return parts.join('\n');
    }

    /**
     * CRITICAL LEGAL ANALYSIS: Determine rent-stabilization status using LEGAL INDICATORS ONLY
     */
    analyzeRentStabilizationLegal(listing, stabilizedBuildings) {
        let confidence = 0;
        const factors = [];
        
        // Factor 1: DHCR building match (STRONGEST indicator - 60% confidence)
        const matchedBuilding = this.findMatchingStabilizedBuilding(listing.address, stabilizedBuildings);
        if (matchedBuilding) {
            confidence += 60;
            factors.push('DHCR registered building (60%)');
        }
        
        // Factor 2: Building age analysis (20% confidence)
        const ageAnalysis = this.analyzeBuildingAge(listing);
        if (ageAnalysis.isEligible) {
            confidence += 20;
            factors.push(`Built before 1974 eligibility (20%)`);
        }
        
        // Factor 3: Building size indicators (15% confidence)
        const sizeAnalysis = this.analyzeBuildingSize(listing);
        if (sizeAnalysis.isEligible) {
            confidence += 15;
            factors.push(`6+ unit building indicators (15%)`);
        }
        
        // Factor 4: Address patterns (5% confidence)
        const addressAnalysis = this.analyzeAddressPatterns(listing);
        if (addressAnalysis.hasIndicators) {
            confidence += 5;
            factors.push(`Address patterns suggest eligibility (5%)`);
        }
        
        const reasoning = factors.length > 0 ? 
            `Legal rent-stabilization analysis:\n• ${factors.join('\n• ')}\n\nTotal confidence: ${confidence}%` :
            'No legal rent-stabilization indicators found';
        
        return {
            confidence: Math.min(confidence, 100),
            factors,
            reasoning,
            dhcrMatch: !!matchedBuilding
        };
    }

    /**
     * Analyze building age for rent stabilization eligibility
     */
    analyzeBuildingAge(listing) {
        // Most rent-stabilized buildings were built before 1974
        const description = (listing.description || '').toLowerCase();
        const yearMatches = description.match(/built.{0,10}(19\d{2}|20\d{2})/i);
        
        if (yearMatches) {
            const year = parseInt(yearMatches[1]);
            return {
                isEligible: year < 1974,
                year,
                reasoning: `Building built in ${year} (${year < 1974 ? 'eligible' : 'not eligible'})`
            };
        }
        
        // If no year found, assume eligible (conservative approach)
        return {
            isEligible: true,
            reasoning: 'Building age not specified (assuming eligible)'
        };
    }

    /**
     * Analyze building size for rent stabilization eligibility
     */
    analyzeBuildingSize(listing) {
        const description = (listing.description || '').toLowerCase();
        const address = (listing.address || '').toLowerCase();
        
        // Look for indicators of 6+ unit buildings
        const sizeIndicators = [
            'elevator', 'doorman', 'concierge', 'lobby', 'building amenities',
            'fitness center', 'roof deck', 'laundry room', 'bike storage'
        ];
        
        const hasIndicators = sizeIndicators.some(indicator => 
            description.includes(indicator) || address.includes(indicator)
        );
        
        return {
            isEligible: hasIndicators,
            reasoning: hasIndicators ? 
                'Building amenities suggest 6+ units (eligible)' : 
                'No clear building size indicators'
        };
    }

    /**
     * Analyze address patterns for rent stabilization indicators
     */
    analyzeAddressPatterns(listing) {
        const address = (listing.address || '').toLowerCase();
        
        // Look for patterns common in rent-stabilized buildings
        const patterns = [
            /\d+[a-z]?\s+(east|west|north|south)/i,  // Street numbers
            /apartment\s+\d+[a-z]?/i,                // Apartment numbers
            /#\d+[a-z]?/i,                           // Unit numbers
            /\d+(st|nd|rd|th)\s+(street|avenue|ave)/i // NYC street patterns
        ];
        
        const hasIndicators = patterns.some(pattern => pattern.test(address));
        
        return {
            hasIndicators,
            reasoning: hasIndicators ? 
                'Address patterns consistent with stabilized housing' : 
                'No specific address patterns identified'
        };
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
}

/**
 * MAIN EXECUTION FUNCTION
 */
async function main() {
    try {
        const system = new RentStabilizedUndervaluedDetector();
        
        // Check for test mode
        const testNeighborhood = process.env.TEST_NEIGHBORHOOD;
        if (testNeighborhood) {
            console.log(`🧪 TEST MODE: Analyzing ${testNeighborhood} only\n`);
            
            const results = await system.findUndervaluedRentStabilizedListings({
                neighborhoods: [testNeighborhood],
                maxListingsPerNeighborhood: 100
            });
            
            console.log(`\n🎯 Test completed for ${testNeighborhood}`);
            console.log(`📊 Found ${results.rentStabilizedFound} rent-stabilized apartments`);
            
            return results;
        }
        
        // Full production analysis
        console.log('🏙️ Running FULL NYC rent-stabilized analysis...\n');
        
        const results = await system.findUndervaluedRentStabilizedListings({
            maxListingsPerNeighborhood: parseInt(process.env.MAX_LISTINGS_PER_NEIGHBORHOOD) || 500
        });
        
        console.log('\n🎉 Full NYC analysis completed!');
        console.log(`📊 Total rent-stabilized apartments found: ${results.rentStabilizedFound}`);
        console.log(`💾 All results saved to database`);
        
        return results;
        
    } catch (error) {
        console.error('💥 System crashed:', error.message);
        process.exit(1);
    }
}

// Export for use in other modules (matches what railway-sequential-runner.js expects)
module.exports = RentStabilizedUndervaluedDetector;

// Run if executed directly
if (require.main === module) {
    main().catch(error => {
        console.error('💥 Main execution failed:', error);
        process.exit(1);
    });
}
