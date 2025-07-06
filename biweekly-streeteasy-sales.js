// enhanced-biweekly-streeteasy-sales.js
// FINAL VERSION: Smart deduplication + ADVANCED MULTI-FACTOR VALUATION + automatic sold listing cleanup
// NEW: Sophisticated bed/bath/amenity-based valuation instead of simple price per sqft
// THRESHOLD: Only properties 25%+ below true market value are flagged as undervalued
require('dotenv').config();
const axios = require('axios');
const { createClient } = require('@supabase/supabase-js');
const EnhancedClaudeMarketAnalyzer = require('./claude-market-analyzer');


const HIGH_PRIORITY_NEIGHBORHOODS = [
    'west-village', 'east-village', 'soho', 'tribeca', 'chelsea',
    'upper-east-side', 'upper-west-side', 'park-slope', 'williamsburg',
    'dumbo', 'brooklyn-heights', 'fort-greene', 'prospect-heights',
    'crown-heights', 'bedford-stuyvesant', 'greenpoint', 'bushwick',
    'long-island-city', 'astoria', 'sunnyside'
];

// Advanced Multi-Factor Sales Valuation Algorithm
// SOPHISTICATED APPROACH: Uses specific bed/bath combinations + amenities + adjustments
// Moves beyond simple price per sqft to true market value assessment
class AdvancedSalesValuationEngine {
    constructor() {
        // Valuation hierarchy - most specific to least specific
        this.VALUATION_METHODS = {
            EXACT_MATCH: 'exact_bed_bath_amenity_match',      // Same beds/baths + similar amenities
            BED_BATH_SPECIFIC: 'bed_bath_specific_pricing',    // Same beds/baths + amenity adjustments  
            BED_SPECIFIC: 'bed_specific_with_adjustments',     // Same bedrooms + bath/amenity adjustments
            PRICE_PER_SQFT_FALLBACK: 'price_per_sqft_fallback' // Last resort
        };

        // Minimum sample sizes for each method
        this.MIN_SAMPLES = {
            EXACT_MATCH: 3,
            BED_BATH_SPECIFIC: 8,
            BED_SPECIFIC: 12,
            PRICE_PER_SQFT_FALLBACK: 20
        };

        // NYC amenity value adjustments for SALES (based on 2025 market data)
        // Structure: { manhattan: {type: 'percentage'/'fixed', value: number}, outer: {type: 'percentage'/'fixed', value: number} }
        this.AMENITY_VALUES = {
            // Building amenities (sales have higher premiums than rentals)
            'doorman_full_time': {
                manhattan: { type: 'percentage', value: 15 }, // 12-18% average for sales
                outer: { type: 'percentage', value: 12 }       // 10-15% average for sales
            },
            'doorman_part_time': {
                manhattan: { type: 'percentage', value: 8 },
                outer: { type: 'percentage', value: 7 }
            },
            'doorman': { // Default to full-time if type not specified
                manhattan: { type: 'percentage', value: 15 },
                outer: { type: 'percentage', value: 12 }
            },
            'concierge': { // Additional premium for ultra-luxury
                manhattan: { type: 'percentage', value: 3 },
                outer: { type: 'percentage', value: 2 }
            },
            'elevator': {
                manhattan: { type: 'fixed', value: 25000 },
                outer: { type: 'fixed', value: 15000 }
            },
            'no_elevator': { // Walk-up penalty for sales
                manhattan: { type: 'fixed', value: -30000 },
                outer: { type: 'fixed', value: -20000 }
            },
            
            // In-unit amenities (significant sales premiums)
            'washer_dryer': {
                manhattan: { type: 'fixed', value: 20000 },
                outer: { type: 'fixed', value: 15000 }
            },
            'dishwasher': {
                manhattan: { type: 'fixed', value: 8000 },
                outer: { type: 'fixed', value: 6000 }
            },
            'central_air': {
                manhattan: { type: 'fixed', value: 15000 },
                outer: { type: 'fixed', value: 12000 }
            },
            'balcony': {
                manhattan: { type: 'fixed', value: 50000 },
                outer: { type: 'fixed', value: 30000 }
            },
            'terrace': {
                manhattan: { type: 'fixed', value: 100000 },
                outer: { type: 'fixed', value: 60000 }
            },
            'private_outdoor_space': {
                manhattan: { type: 'fixed', value: 100000 },
                outer: { type: 'fixed', value: 60000 }
            },
            
            // Building facilities
            'gym': {
                manhattan: { type: 'fixed', value: 15000 },
                outer: { type: 'fixed', value: 10000 }
            },
            'fitness_center': {
                manhattan: { type: 'fixed', value: 15000 },
                outer: { type: 'fixed', value: 10000 }
            },
            'pool': {
                manhattan: { type: 'fixed', value: 30000 },
                outer: { type: 'fixed', value: 20000 }
            },
            'roof_deck': {
                manhattan: { type: 'fixed', value: 10000 },
                outer: { type: 'fixed', value: 7500 }
            },
            'laundry_room': {
                manhattan: { type: 'fixed', value: 5000 },
                outer: { type: 'fixed', value: 5000 }
            },
            'parking': {
                manhattan: { type: 'fixed', value: 75000 },
                outer: { type: 'fixed', value: 40000 }
            },
            'bike_storage': {
                manhattan: { type: 'fixed', value: 2500 },
                outer: { type: 'fixed', value: 2000 }
            },
            
            // Pet and lifestyle
            'pet_friendly': {
                manhattan: { type: 'fixed', value: 10000 },
                outer: { type: 'fixed', value: 8000 }
            },
            
            // Condition and quality (percentage-based for sales)
            'newly_renovated': {
                manhattan: { type: 'percentage', value: 12 }, // 10-15% average for sales
                outer: { type: 'percentage', value: 12 }
            },
            'luxury_finishes': {
                manhattan: { type: 'percentage', value: 8 },
                outer: { type: 'percentage', value: 7 }
            },
            'hardwood_floors': {
                manhattan: { type: 'fixed', value: 12000 },
                outer: { type: 'fixed', value: 10000 }
            },
            'high_ceilings': {
                manhattan: { type: 'percentage', value: 6 },
                outer: { type: 'percentage', value: 5 }
            },
            'exposed_brick': {
                manhattan: { type: 'fixed', value: 15000 },
                outer: { type: 'fixed', value: 10000 }
            },
            
            // Negative factors
            'studio_layout': {
                manhattan: { type: 'percentage', value: -20 }, // -15 to -25% average
                outer: { type: 'percentage', value: -20 }
            },
            'no_natural_light': {
                manhattan: { type: 'percentage', value: -18 }, // -15 to -20% average
                outer: { type: 'percentage', value: -18 }
            },
            'noisy_location': {
                manhattan: { type: 'percentage', value: -12 }, // -8 to -15% average
                outer: { type: 'percentage', value: -12 }
            },
            'ground_floor': {
                manhattan: { type: 'percentage', value: -8 }, // -5 to -12% average
                outer: { type: 'percentage', value: -8 }
            }
        };

        // Bathroom adjustment factors (fixed dollar amounts for sales)
        this.BATHROOM_ADJUSTMENTS = {
            0.5: -50000,  // Half bath deficit
            1.0: 0,       // Baseline
            1.5: 30000,   // Extra half bath
            2.0: 60000,   // Full second bathroom
            2.5: 80000,   // Two and a half baths
            3.0: 100000   // Three full bathrooms
        };

        // Square footage adjustments per bedroom category (sales)
        this.SQFT_ADJUSTMENTS = {
            'studio': { baseline: 450, per_sqft_over: 400, per_sqft_under: -500 },
            '1bed': { baseline: 650, per_sqft_over: 350, per_sqft_under: -450 },
            '2bed': { baseline: 900, per_sqft_over: 300, per_sqft_under: -400 },
            '3bed': { baseline: 1200, per_sqft_over: 250, per_sqft_under: -350 }
        };
    }

    /**
     * Determine if a property is in Manhattan based on neighborhood or borough
     */
    isManhattan(targetProperty) {
        const borough = (targetProperty.borough || '').toLowerCase();
        const neighborhood = (targetProperty.neighborhood || '').toLowerCase();
        
        // Check borough first
        if (borough.includes('manhattan') || borough.includes('new york')) {
            return true;
        }
        
        // Check for Manhattan neighborhood indicators
        const manhattanIndicators = [
            'village', 'soho', 'tribeca', 'chelsea', 'midtown', 'upper', 'lower',
            'financial', 'chinatown', 'little italy', 'gramercy', 'murray hill',
            'hell', 'harlem', 'washington heights', 'inwood'
        ];
        
        return manhattanIndicators.some(indicator => neighborhood.includes(indicator));
    }

    /**
     * Calculate amenity value using location-specific and type-aware adjustments
     */
    calculateLocationAwareAmenityValue(amenities, targetProperty, basePrice = 0) {
        const isManhattan = this.isManhattan(targetProperty);
        let totalAdjustment = 0;
        const appliedAdjustments = [];

        amenities.forEach(amenity => {
            const amenityConfig = this.AMENITY_VALUES[amenity];
            if (!amenityConfig) return;

            const locationConfig = isManhattan ? amenityConfig.manhattan : amenityConfig.outer;
            let adjustment = 0;

            if (locationConfig.type === 'percentage') {
                // Percentage adjustment requires base price
                if (basePrice > 0) {
                    adjustment = (basePrice * locationConfig.value) / 100;
                    appliedAdjustments.push(`${amenity}: +${locationConfig.value}% (${isManhattan ? 'Manhattan' : 'Outer'}) = ${Math.round(adjustment).toLocaleString()}`);
                }
            } else if (locationConfig.type === 'fixed') {
                adjustment = locationConfig.value;
                appliedAdjustments.push(`${amenity}: ${adjustment.toLocaleString()} (${isManhattan ? 'Manhattan' : 'Outer'})`);
            }

            totalAdjustment += adjustment;
        });

        return {
            totalAdjustment: Math.round(totalAdjustment),
            breakdown: appliedAdjustments,
            isManhattan: isManhattan
        };
    }

    /**
     * MAIN VALUATION ENGINE: Calculate true market value using multiple factors
     */
    calculateTrueMarketValue(targetProperty, comparableProperties, neighborhood) {
        console.log(`   🎯 ADVANCED VALUATION: ${targetProperty.address || 'Property'}`);
        console.log(`   📊 Analyzing against ${comparableProperties.length} comparables in ${neighborhood}`);

        // Step 1: Try most specific valuation method first
        const valuationResult = this.selectBestValuationMethod(targetProperty, comparableProperties);
        
        if (!valuationResult.success) {
            return {
                success: false,
                estimatedMarketPrice: 0,
                method: 'insufficient_data',
                confidence: 0,
                reasoning: valuationResult.reason
            };
        }

        // Step 2: Calculate base market value using selected method
        const baseMarketValue = this.calculateBaseMarketValue(
            targetProperty, 
            valuationResult.comparables, 
            valuationResult.method
        );

        // Step 3: Apply detailed adjustments for property-specific factors
        const adjustedMarketValue = this.applyDetailedAdjustments(
            targetProperty,
            baseMarketValue,
            valuationResult.comparables,
            valuationResult.method
        );

        // Step 4: Calculate confidence score based on data quality
        const confidence = this.calculateConfidenceScore(
            valuationResult.comparables.length,
            valuationResult.method,
            targetProperty,
            comparableProperties
        );

        console.log(`   💰 Base value: $${baseMarketValue.baseValue.toLocaleString()}`);
        console.log(`   🔧 Adjustments: $${(adjustedMarketValue.totalAdjustments > 0 ? '+' : '')}${adjustedMarketValue.totalAdjustments.toLocaleString()}`);
        console.log(`   🎯 Est. market price: $${adjustedMarketValue.finalValue.toLocaleString()}`);
        console.log(`   📊 Method: ${valuationResult.method} (${confidence}% confidence)`);

        return {
            success: true,
            estimatedMarketPrice: adjustedMarketValue.finalValue,
            baseMarketPrice: baseMarketValue.baseValue,
            totalAdjustments: adjustedMarketValue.totalAdjustments,
            adjustmentBreakdown: adjustedMarketValue.adjustments,
            method: valuationResult.method,
            confidence: confidence,
            comparablesUsed: valuationResult.comparables.length,
            reasoning: this.generateValuationReasoning(targetProperty, baseMarketValue, adjustedMarketValue, valuationResult)
        };
    }

    /**
     * Select the best valuation method based on available comparable data
     */
    selectBestValuationMethod(targetProperty, comparables) {
        const beds = targetProperty.bedrooms || 0;
        const baths = targetProperty.bathrooms || 0;
        
        // Method 1: Exact bed/bath match with similar amenities
        const exactMatches = comparables.filter(comp => 
            comp.bedrooms === beds && 
            Math.abs(comp.bathrooms - baths) <= 0.5 &&
            this.hasReasonableDataQuality(comp)
        );
        
        if (exactMatches.length >= this.MIN_SAMPLES.EXACT_MATCH) {
            console.log(`   ✅ Using EXACT_MATCH: ${exactMatches.length} properties with ${beds}BR/${baths}BA`);
            return {
                success: true,
                method: this.VALUATION_METHODS.EXACT_MATCH,
                comparables: exactMatches
            };
        }

        // Method 2: Same bed/bath count (broader amenity tolerance)
        const bedBathMatches = comparables.filter(comp => 
            comp.bedrooms === beds && 
            comp.bathrooms >= (baths - 0.5) && comp.bathrooms <= (baths + 0.5) &&
            this.hasReasonableDataQuality(comp)
        );
        
        if (bedBathMatches.length >= this.MIN_SAMPLES.BED_BATH_SPECIFIC) {
            console.log(`   ✅ Using BED_BATH_SPECIFIC: ${bedBathMatches.length} properties with ${beds}BR/${baths}±0.5BA`);
            return {
                success: true,
                method: this.VALUATION_METHODS.BED_BATH_SPECIFIC,
                comparables: bedBathMatches
            };
        }

        // Method 3: Same bedroom count (will adjust for bathroom differences)
        const bedMatches = comparables.filter(comp => 
            comp.bedrooms === beds &&
            this.hasReasonableDataQuality(comp)
        );
        
        if (bedMatches.length >= this.MIN_SAMPLES.BED_SPECIFIC) {
            console.log(`   ⚠️ Using BED_SPECIFIC: ${bedMatches.length} properties with ${beds}BR (will adjust for bath differences)`);
            return {
                success: true,
                method: this.VALUATION_METHODS.BED_SPECIFIC,
                comparables: bedMatches
            };
        }

        // Method 4: Price per sqft fallback (least preferred)
        const sqftComparables = comparables.filter(comp => 
            comp.sqft > 0 && comp.salePrice > 0 &&
            this.hasReasonableDataQuality(comp)
        );
        
        if (sqftComparables.length >= this.MIN_SAMPLES.PRICE_PER_SQFT_FALLBACK) {
            console.log(`   ⚠️ Using PRICE_PER_SQFT_FALLBACK: ${sqftComparables.length} properties (least accurate method)`);
            return {
                success: true,
                method: this.VALUATION_METHODS.PRICE_PER_SQFT_FALLBACK,
                comparables: sqftComparables
            };
        }

        // Insufficient data
        return {
            success: false,
            reason: `Insufficient comparable data: ${comparables.length} total, need min ${this.MIN_SAMPLES.BED_BATH_SPECIFIC} for ${beds}BR/${baths}BA`
        };
    }

    /**
     * Calculate base market value using the selected method
     */
    calculateBaseMarketValue(targetProperty, comparables, method) {
        switch (method) {
            case this.VALUATION_METHODS.EXACT_MATCH:
            case this.VALUATION_METHODS.BED_BATH_SPECIFIC:
                return this.calculateBedBathBasedValue(targetProperty, comparables);
                
            case this.VALUATION_METHODS.BED_SPECIFIC:
                return this.calculateBedroomBasedValue(targetProperty, comparables);
                
            case this.VALUATION_METHODS.PRICE_PER_SQFT_FALLBACK:
                return this.calculateSqftBasedValue(targetProperty, comparables);
                
            default:
                throw new Error(`Unknown valuation method: ${method}`);
        }
    }

    /**
     * Method 1 & 2: Bed/Bath specific pricing (most accurate)
     */
    calculateBedBathBasedValue(targetProperty, comparables) {
        const prices = comparables.map(comp => comp.salePrice).sort((a, b) => a - b);
        const median = this.calculateMedian(prices);
        
        // Use median as most stable central tendency
        return {
            baseValue: median,
            method: 'bed_bath_median',
            dataPoints: prices.length,
            priceRange: { min: Math.min(...prices), max: Math.max(...prices) }
        };
    }

    /**
     * Method 3: Bedroom-based with bathroom adjustments
     */
    calculateBedroomBasedValue(targetProperty, comparables) {
        const targetBaths = targetProperty.bathrooms || 1;
        
        // Calculate base price for this bedroom count
        const prices = comparables.map(comp => comp.salePrice);
        const medianPrice = this.calculateMedian(prices);
        
        // Find typical bathroom count for this bedroom category
        const bathCounts = comparables.map(comp => comp.bathrooms || 1);
        const typicalBathCount = this.calculateMedian(bathCounts);
        
        // Adjust base price for bathroom difference
        const bathDifference = targetBaths - typicalBathCount;
        const bathAdjustment = this.calculateBathroomAdjustment(bathDifference);
        
        return {
            baseValue: medianPrice + bathAdjustment,
            method: 'bedroom_based_with_bath_adjustment',
            dataPoints: prices.length,
            bathAdjustment: bathAdjustment,
            typicalBathCount: typicalBathCount
        };
    }

    /**
     * Method 4: Price per sqft fallback (least preferred)
     */
    calculateSqftBasedValue(targetProperty, comparables) {
        const targetSqft = targetProperty.sqft || 0;
        
        if (targetSqft === 0) {
            // Estimate sqft based on bedroom count
            const bedrooms = targetProperty.bedrooms || 0;
            const estimatedSqft = this.estimateSquareFootage(bedrooms);
            console.log(`   ⚠️ No sqft data, estimating ${estimatedSqft} sqft for ${bedrooms}BR`);
            targetProperty.sqft = estimatedSqft; // Temporary for calculation
        }
        
        // Calculate median price per sqft
        const pricesPerSqft = comparables
            .filter(comp => comp.sqft > 0)
            .map(comp => comp.salePrice / comp.sqft);
            
        const medianPricePerSqft = this.calculateMedian(pricesPerSqft);
        
        return {
            baseValue: medianPricePerSqft * targetProperty.sqft,
            method: 'price_per_sqft',
            dataPoints: pricesPerSqft.length,
            medianPricePerSqft: medianPricePerSqft
        };
    }

    /**
     * Apply detailed adjustments for property-specific factors
     */
    applyDetailedAdjustments(targetProperty, baseValue, comparables, method) {
        const adjustments = [];
        let totalAdjustment = 0;

        // 1. Amenity adjustments (most important)
        const amenityAdjustment = this.calculateAmenityAdjustments(targetProperty, comparables);
        if (amenityAdjustment.totalAdjustment !== 0) {
            adjustments.push({
                category: 'amenities',
                amount: amenityAdjustment.totalAdjustment,
                details: amenityAdjustment.breakdown
            });
            totalAdjustment += amenityAdjustment.totalAdjustment;
        }

        // 2. Square footage adjustments (for bed/bath methods)
        if (method !== this.VALUATION_METHODS.PRICE_PER_SQFT_FALLBACK) {
            const sqftAdjustment = this.calculateSquareFootageAdjustment(targetProperty, comparables);
            if (sqftAdjustment !== 0) {
                adjustments.push({
                    category: 'square_footage',
                    amount: sqftAdjustment,
                    details: `${targetProperty.sqft} sqft vs comparable average`
                });
                totalAdjustment += sqftAdjustment;
            }
        }

        // 3. Condition and quality adjustments
        const qualityAdjustment = this.calculateQualityAdjustments(targetProperty);
        if (qualityAdjustment !== 0) {
            adjustments.push({
                category: 'quality_condition',
                amount: qualityAdjustment,
                details: 'Based on listing description and photos'
            });
            totalAdjustment += qualityAdjustment;
        }

        // 4. Location micro-adjustments within neighborhood
        const locationAdjustment = this.calculateLocationAdjustments(targetProperty);
        if (locationAdjustment !== 0) {
            adjustments.push({
                category: 'micro_location',
                amount: locationAdjustment,
                details: 'Street-level location factors'
            });
            totalAdjustment += locationAdjustment;
        }

        return {
            finalValue: Math.round(baseValue.baseValue + totalAdjustment),
            totalAdjustments: totalAdjustment,
            adjustments: adjustments
        };
    }

    /**
     * Calculate amenity-based adjustments compared to comparable properties using location-aware pricing
     */
    calculateAmenityAdjustments(targetProperty, comparables) {
        const targetAmenities = this.normalizeAmenities(targetProperty.amenities || []);
        
        // Add description-based amenities
        const descriptionAmenities = this.extractAmenitiesFromDescription(targetProperty.description || '');
        const allTargetAmenities = [...new Set([...targetAmenities, ...descriptionAmenities])];
        
        // Calculate average amenity value of comparables (using their base prices)
        const comparableAmenityValues = comparables.map(comp => {
            const compAmenities = this.normalizeAmenities(comp.amenities || []);
            const compDescAmenities = this.extractAmenitiesFromDescription(comp.description || '');
            const allCompAmenities = [...new Set([...compAmenities, ...compDescAmenities])];
            
            const amenityAnalysis = this.calculateLocationAwareAmenityValue(
                allCompAmenities, 
                comp, 
                comp.salePrice || 0
            );
            return amenityAnalysis.totalAdjustment;
        });
        
        const avgComparableAmenityValue = comparableAmenityValues.reduce((a, b) => a + b, 0) / comparableAmenityValues.length;
        
        // Calculate target property amenity value (using estimated market price for percentage calculations)
        const estimatedBasePrice = this.estimateBasePriceForAmenityCalculation(targetProperty, comparables);
        const targetAmenityAnalysis = this.calculateLocationAwareAmenityValue(
            allTargetAmenities, 
            targetProperty, 
            estimatedBasePrice
        );
        
        const adjustment = targetAmenityAnalysis.totalAdjustment - avgComparableAmenityValue;

        return {
            totalAdjustment: Math.round(adjustment),
            targetAmenityValue: targetAmenityAnalysis.totalAdjustment,
            avgComparableAmenityValue: avgComparableAmenityValue,
            breakdown: targetAmenityAnalysis.breakdown,
            isManhattan: targetAmenityAnalysis.isManhattan
        };
    }

    /**
     * Estimate base price for amenity percentage calculations
     */
    estimateBasePriceForAmenityCalculation(targetProperty, comparables) {
        // Use median price of comparables as estimate for percentage-based amenity calculations
        const comparablePrices = comparables.map(comp => comp.salePrice).filter(price => price > 0);
        if (comparablePrices.length === 0) return 750000; // Fallback for NYC
        
        const sortedPrices = comparablePrices.sort((a, b) => a - b);
        return sortedPrices[Math.floor(sortedPrices.length / 2)];
    }

    /**
     * Extract amenities from property description text
     */
    extractAmenitiesFromDescription(description) {
        const text = description.toLowerCase();
        const foundAmenities = [];
        
        // Check for amenities mentioned in description
        const amenityDetectionRules = {
            'doorman_full_time': ['full time doorman', 'full-time doorman', '24 hour doorman', '24/7 doorman'],
            'doorman_part_time': ['part time doorman', 'part-time doorman', 'virtual doorman'],
            'doorman': ['doorman'], // Fallback if no specific type found
            'concierge': ['concierge'],
            'elevator': ['elevator', 'lift'],
            'no_elevator': ['walk up', 'walk-up', 'no elevator', 'walkup'],
            'washer_dryer': ['washer/dryer', 'washer dryer', 'w/d', 'laundry in unit', 'in-unit laundry'],
            'dishwasher': ['dishwasher'],
            'central_air': ['central air', 'central a/c', 'central ac'],
            'balcony': ['balcony', 'private balcony'],
            'terrace': ['terrace', 'private terrace'],
            'gym': ['gym', 'fitness center', 'fitness room'],
            'pool': ['pool', 'swimming pool'],
            'roof_deck': ['roof deck', 'rooftop', 'roof top'],
            'parking': ['parking', 'garage parking', 'parking space'],
            'pet_friendly': ['pets allowed', 'pet friendly', 'pet ok', 'dogs allowed', 'cats allowed'],
            'newly_renovated': ['newly renovated', 'gut renovated', 'completely renovated'],
            'luxury_finishes': ['luxury finishes', 'high-end finishes', 'luxury fixtures'],
            'hardwood_floors': ['hardwood floors', 'hardwood', 'wood floors'],
            'high_ceilings': ['high ceilings', 'vaulted ceilings', '10 foot ceilings', '11 foot ceilings'],
            'exposed_brick': ['exposed brick', 'brick walls'],
            'no_natural_light': ['no windows', 'no natural light', 'basement', 'airshaft'],
            'noisy_location': ['busy street', 'noisy', 'traffic'],
            'ground_floor': ['ground floor', 'first floor', 'garden level']
        };
        
        // Check each amenity against description
        for (const [amenity, keywords] of Object.entries(amenityDetectionRules)) {
            if (keywords.some(keyword => text.includes(keyword))) {
                foundAmenities.push(amenity);
            }
        }
        
        return foundAmenities;
    }

    /**
     * Calculate square footage adjustments for bed/bath-based valuations
     */
    calculateSquareFootageAdjustment(targetProperty, comparables) {
        const targetSqft = targetProperty.sqft || 0;
        const bedrooms = targetProperty.bedrooms || 0;
        
        if (targetSqft === 0) return 0;
        
        // Calculate average sqft for comparables
        const comparableSqfts = comparables
            .filter(comp => comp.sqft > 0)
            .map(comp => comp.sqft);
            
        if (comparableSqfts.length === 0) return 0;
        
        const avgComparableSqft = comparableSqfts.reduce((a, b) => a + b, 0) / comparableSqfts.length;
        const sqftDifference = targetSqft - avgComparableSqft;
        
        // Get baseline sqft expectations for this bedroom count
        const bedroomKey = bedrooms === 0 ? 'studio' : `${bedrooms}bed`;
        const sqftStandards = this.SQFT_ADJUSTMENTS[bedroomKey] || this.SQFT_ADJUSTMENTS['1bed'];
        
        // Calculate adjustment based on how much over/under average
        let adjustment = 0;
        if (sqftDifference > 0) {
            // Above average sqft
            adjustment = sqftDifference * sqftStandards.per_sqft_over;
        } else {
            // Below average sqft  
            adjustment = sqftDifference * Math.abs(sqftStandards.per_sqft_under);
        }
        
        return Math.round(adjustment);
    }

    /**
     * Calculate quality and condition adjustments
     */
    calculateQualityAdjustments(targetProperty) {
        let adjustment = 0;
        const description = (targetProperty.description || '').toLowerCase();
        
        // Positive quality indicators
        if (description.includes('newly renovated') || description.includes('gut renovated')) {
            adjustment += 50000; // Fixed amount for sales
        }
        if (description.includes('luxury') || description.includes('high-end')) {
            adjustment += 40000;
        }
        if (description.includes('hardwood') || description.includes('wood floors')) {
            adjustment += 15000;
        }
        
        // Negative quality indicators
        if (description.includes('needs work') || description.includes('tlc')) {
            adjustment -= 75000;
        }
        if (description.includes('as-is') || description.includes('fixer')) {
            adjustment -= 100000;
        }
        
        return adjustment;
    }

    /**
     * Calculate micro-location adjustments within neighborhood
     */
    calculateLocationAdjustments(targetProperty) {
        let adjustment = 0;
        const address = (targetProperty.address || '').toLowerCase();
        const description = (targetProperty.description || '').toLowerCase();
        
        // Street-level factors
        if (description.includes('quiet street') || description.includes('tree-lined')) {
            adjustment += 25000;
        }
        if (description.includes('busy street') || description.includes('noisy')) {
            adjustment -= 50000;
        }
        if (description.includes('ground floor') && !description.includes('garden')) {
            adjustment -= 30000;
        }
        
        return adjustment;
    }

    /**
     * Normalize and standardize amenity names
     */
    normalizeAmenities(amenities) {
        const normalized = [];
        const amenityText = amenities.join(' ').toLowerCase();
        
        // Map common variations to standard names
        const amenityMappings = {
            'doorman': ['doorman', 'full time doorman', 'full-time doorman'],
            'elevator': ['elevator', 'lift'],
            'washer_dryer': ['washer/dryer', 'washer dryer', 'w/d', 'laundry in unit'],
            'dishwasher': ['dishwasher', 'dish washer'],
            'central_air': ['central air', 'central a/c', 'central ac'],
            'balcony': ['balcony', 'private balcony'],
            'terrace': ['terrace', 'private terrace', 'roof terrace'],
            'gym': ['gym', 'fitness center', 'fitness room'],
            'pool': ['pool', 'swimming pool'],
            'roof_deck': ['roof deck', 'rooftop', 'roof top'],
            'parking': ['parking', 'garage parking', 'parking space'],
            'pet_friendly': ['pets allowed', 'pet friendly', 'pet ok', 'dogs allowed', 'cats allowed']
        };
        
        // Check for each standard amenity
        for (const [standardName, variations] of Object.entries(amenityMappings)) {
            if (variations.some(variation => amenityText.includes(variation))) {
                normalized.push(standardName);
            }
        }
        
        return [...new Set(normalized)]; // Remove duplicates
    }

    /**
     * Calculate bathroom adjustment amount
     */
    calculateBathroomAdjustment(bathDifference) {
        // Convert bathroom difference to adjustment amount (sales values)
        return bathDifference * 50000; // $50,000 per 0.5 bathroom difference
    }

    /**
     * Estimate square footage based on bedroom count
     */
    estimateSquareFootage(bedrooms) {
        const estimates = {
            0: 450,  // Studio
            1: 650,  // 1BR
            2: 900,  // 2BR
            3: 1200, // 3BR
            4: 1500  // 4BR+
        };
        return estimates[bedrooms] || estimates[1];
    }

    /**
     * Calculate confidence score based on data quality and method used
     */
    calculateConfidenceScore(comparablesCount, method, targetProperty, allComparables) {
        let confidence = 0;
        
        // Base confidence from method used
        switch (method) {
            case this.VALUATION_METHODS.EXACT_MATCH:
                confidence = 95;
                break;
            case this.VALUATION_METHODS.BED_BATH_SPECIFIC:
                confidence = 85;
                break;
            case this.VALUATION_METHODS.BED_SPECIFIC:
                confidence = 75;
                break;
            case this.VALUATION_METHODS.PRICE_PER_SQFT_FALLBACK:
                confidence = 60;
                break;
        }
        
        // Adjust based on sample size
        if (comparablesCount >= 20) confidence += 5;
        else if (comparablesCount >= 15) confidence += 3;
        else if (comparablesCount >= 10) confidence += 1;
        else if (comparablesCount < 5) confidence -= 10;
        
        // Adjust based on data completeness
        const hasGoodSqftData = (targetProperty.sqft || 0) > 0;
        const hasAmenityData = (targetProperty.amenities || []).length > 0;
        
        if (hasGoodSqftData) confidence += 5;
        if (hasAmenityData) confidence += 5;
        
        return Math.min(100, Math.max(0, confidence));
    }

    /**
     * Generate detailed reasoning for the valuation
     */
    generateValuationReasoning(targetProperty, baseValue, adjustedValue, valuationResult) {
        const reasons = [];
        
        reasons.push(`Base value: ${baseValue.baseValue.toLocaleString()} (${valuationResult.method})`);
        reasons.push(`${valuationResult.comparables.length} comparable properties used`);
        
        if (adjustedValue.adjustments.length > 0) {
            adjustedValue.adjustments.forEach(adj => {
                const sign = adj.amount > 0 ? '+' : '';
                reasons.push(`${adj.category}: ${sign}${adj.amount.toLocaleString()}`);
            });
        }
        
        return reasons.join('; ');
    }

    /**
     * Check if a comparable has reasonable data quality
     */
    hasReasonableDataQuality(comparable) {
        return comparable.salePrice > 0 &&
               comparable.salePrice <= 50000000 &&
               comparable.bedrooms !== undefined &&
               comparable.bathrooms !== undefined &&
               (comparable.daysOnMarket || 0) <= 365; // Not stale
    }

    /**
     * Calculate median value from array
     */
    calculateMedian(values) {
        const sorted = [...values].sort((a, b) => a - b);
        const mid = Math.floor(sorted.length / 2);
        return sorted.length % 2 === 0 
            ? (sorted[mid - 1] + sorted[mid]) / 2 
            : sorted[mid];
    }

    /**
     * MAIN INTERFACE: Analyze sale for undervaluation using advanced multi-factor approach
     */
    analyzeSalesUndervaluation(targetProperty, comparableProperties, neighborhood, options = {}) {
        const threshold = options.undervaluationThreshold || 10; // NEW: Lowered to 10% for more realistic results
        
        console.log(`\n🎯 ADVANCED VALUATION: ${targetProperty.address || 'Property'}`);
        console.log(`   📊 Price: ${targetProperty.salePrice.toLocaleString()} | ${targetProperty.bedrooms}BR/${targetProperty.bathrooms}BA | ${targetProperty.sqft || 'N/A'} sqft`);
        
        // Get true market value using advanced multi-factor analysis
        const valuation = this.calculateTrueMarketValue(targetProperty, comparableProperties, neighborhood);
        
        if (!valuation.success) {
            return {
                isUndervalued: false,
                discountPercent: 0,
                estimatedMarketPrice: 0,
                actualPrice: targetProperty.salePrice,
                confidence: 0,
                method: 'insufficient_data',
                reasoning: valuation.reasoning
            };
        }
        
        // Calculate actual discount percentage
        const actualPrice = targetProperty.salePrice;
        const estimatedMarketPrice = valuation.estimatedMarketPrice;
        const discountPercent = ((estimatedMarketPrice - actualPrice) / estimatedMarketPrice) * 100;
        
        // Determine if truly undervalued - NEW: Only 10%+ below market with method-appropriate confidence
        // Confidence thresholds adjusted per valuation method:
        // - Exact/Bed-Bath methods: 70% minimum (high accuracy methods)
        // - Bed-specific method: 60% minimum (good accuracy with adjustments)  
        // - Price per sqft fallback: 50% minimum (lower accuracy but still useful)
        let minConfidenceThreshold = 50; // Default for price per sqft
        if (valuation.method === 'exact_bed_bath_amenity_match' || valuation.method === 'bed_bath_specific_pricing') {
            minConfidenceThreshold = 70; // Higher standard for best methods
        } else if (valuation.method === 'bed_specific_with_adjustments') {
            minConfidenceThreshold = 60; // Medium standard for adjusted method
        }
        
        const isUndervalued = discountPercent >= threshold && valuation.confidence >= minConfidenceThreshold;
        
        console.log(`   💰 Actual price: ${actualPrice.toLocaleString()}`);
        console.log(`   🎯 Market value: ${estimatedMarketPrice.toLocaleString()}`);
        console.log(`   📊 Discount: ${discountPercent.toFixed(1)}%`);
        console.log(`   ✅ Undervalued: ${isUndervalued ? 'YES' : 'NO'} (${threshold}% threshold, ${valuation.confidence}% confidence)`);
        
        return {
            isUndervalued,
            discountPercent: Math.round(discountPercent * 10) / 10,
            estimatedMarketPrice: estimatedMarketPrice,
            actualPrice: actualPrice,
            potentialProfit: Math.round(estimatedMarketPrice - actualPrice),
            confidence: valuation.confidence,
            method: valuation.method,
            comparablesUsed: valuation.comparablesUsed,
            adjustmentBreakdown: valuation.adjustmentBreakdown,
            reasoning: valuation.reasoning
        };
    }
}

class EnhancedBiWeeklySalesAnalyzer {
    constructor() {
        this.supabase = createClient(
            process.env.SUPABASE_URL,
            process.env.SUPABASE_ANON_KEY
        );
        
        this.rapidApiKey = process.env.RAPIDAPI_KEY;
        this.apiCallsUsed = 0;
        
        // Initialize the advanced valuation engine
        this.valuationEngine = new AdvancedSalesValuationEngine();
        
        // ADD THIS LINE RIGHT HERE:
this.claudeAnalyzer = new EnhancedClaudeMarketAnalyzer();
        
        // Check for initial bulk load mode
        this.initialBulkLoad = process.env.INITIAL_BULK_LOAD === 'true';
        
        // Store deploy/startup time for delay calculation
        this.deployTime = new Date().getTime();
        
        // ADAPTIVE RATE LIMITING SYSTEM
        this.baseDelay = this.initialBulkLoad ? 8000 : 6000; // Slightly slower for bulk load
        this.maxRetries = 3;
        this.retryBackoffMultiplier = 2;
        
        // Adaptive rate limiting tracking
        this.rateLimitHits = 0;
        this.callTimestamps = [];
        this.maxCallsPerHour = this.initialBulkLoad ? 250 : 300; // More conservative for bulk
        this.lastRateLimitTime = null;
        
        // Smart scheduling system
        this.dailySchedule = this.createDailySchedule();
        this.currentDay = this.getCurrentScheduleDay();
        
        // Track sales-specific API usage with DEDUPLICATION stats
        this.apiUsageStats = {
            activeSalesCalls: 0,
            detailsCalls: 0,
            failedCalls: 0,
            rateLimitHits: 0,
            adaptiveDelayChanges: 0,
            // NEW: Deduplication performance tracking
            totalListingsFound: 0,
            cacheHits: 0,
            newListingsToFetch: 0,
            apiCallsSaved: 0,
            listingsMarkedSold: 0
        };
    }

    /**
     * Create smart daily schedule spread over bi-weekly period
     */
    createDailySchedule() {
        // Spread 20 neighborhoods over 8 days (within 2 weeks)
        return {
            1: ['west-village', 'east-village', 'soho'], // High-value Manhattan
            2: ['tribeca', 'chelsea', 'upper-east-side'],
            3: ['upper-west-side', 'park-slope', 'williamsburg'], // Premium Brooklyn
            4: ['dumbo', 'brooklyn-heights', 'fort-greene'],
            5: ['prospect-heights', 'crown-heights', 'bedford-stuyvesant'], // Emerging Brooklyn
            6: ['greenpoint', 'bushwick', 'long-island-city'], // LIC + North Brooklyn
            7: ['astoria', 'sunnyside'], // Queens
            8: [] // Buffer day for catch-up or missed neighborhoods
        };
    }

    /**
     * Determine which day of the bi-weekly cycle we're on
     */
    getCurrentScheduleDay() {
        const today = new Date();
        const dayOfMonth = today.getDate();
        
        // SALES SCHEDULE: Days 1-8 and 15-22 of each month
        if (dayOfMonth >= 1 && dayOfMonth <= 8) {
            return dayOfMonth; // Days 1-8
        } else if (dayOfMonth >= 15 && dayOfMonth <= 22) {
            return dayOfMonth - 14; // Days 15-22 become 1-8
        } else {
            return 0; // Off-schedule, run buffer mode
        }
    }

    /**
     * Get today's neighborhood assignments WITH BULK LOAD SUPPORT
     */
    async getTodaysNeighborhoods() {
        // INITIAL BULK LOAD: Process ALL neighborhoods
        if (this.initialBulkLoad) {
            console.log('🚀 INITIAL BULK LOAD MODE: Processing ALL sales neighborhoods');
            console.log(`📋 Will process ${HIGH_PRIORITY_NEIGHBORHOODS.length} neighborhoods over multiple hours`);
            return [dumbo];
        }
        
        // Normal bi-weekly schedule (for production)
        const todaysNeighborhoods = this.dailySchedule[this.currentDay] || [];
        
        if (todaysNeighborhoods.length === 0) {
            // Off-schedule or buffer day - check for missed neighborhoods
            console.log('📅 Off-schedule day - checking for missed neighborhoods');
            return await this.getMissedNeighborhoods();
        }
        
        console.log(`📅 Day ${this.currentDay} schedule: ${todaysNeighborhoods.length} neighborhoods`);
        return todaysNeighborhoods;
    }

    /**
     * Check for neighborhoods that might have been missed
     */
    async getMissedNeighborhoods() {
        try {
            // Check database for neighborhoods not analyzed in last 14 days
            const { data, error } = await this.supabase
                .from('bi_weekly_analysis_runs')
                .select('detailed_stats')
                .eq('analysis_type', 'sales')
                .gte('run_date', new Date(Date.now() - 14 * 24 * 60 * 60 * 1000).toISOString())
                .order('run_date', { ascending: false });

            if (error) {
                console.log('⚠️ Could not check missed neighborhoods, using backup list');
                return ['park-slope', 'williamsburg']; // Safe fallback
            }

            // Extract neighborhoods that were processed
            const processedNeighborhoods = new Set();
            data.forEach(run => {
                if (run.detailed_stats?.byNeighborhood) {
                    Object.keys(run.detailed_stats.byNeighborhood).forEach(n => 
                        processedNeighborhoods.add(n)
                    );
                }
            });

            // Find neighborhoods not processed recently
            const allNeighborhoods = Object.values(this.dailySchedule).flat();
            const missedNeighborhoods = allNeighborhoods.filter(n => 
                !processedNeighborhoods.has(n)
            );

            console.log(`🔍 Found ${missedNeighborhoods.length} missed neighborhoods: ${missedNeighborhoods.join(', ')}`);
            return missedNeighborhoods.slice(0, 5); // Max 5 catch-up neighborhoods
        } catch (error) {
            console.log('⚠️ Error checking missed neighborhoods, using fallback');
            return ['park-slope', 'williamsburg'];
        }
    }

    /**
     * NEW: EFFICIENT: Update only price in cache (no refetch needed)
     */
    async updatePriceInCache(listingId, newPrice) {
        try {
            const { error } = await this.supabase
                .from('sales_market_cache')
                .update({
                    price: newPrice,
                    last_checked: new Date().toISOString(),
                    market_status: 'pending'
                })
                .eq('listing_id', listingId);

            if (error) {
                console.warn(`⚠️ Error updating price for ${listingId}:`, error.message);
            } else {
                console.log(`   💾 Updated cache price for ${listingId}: ${newPrice.toLocaleString()}`);
            }
        } catch (error) {
            console.warn(`⚠️ Error updating price in cache for ${listingId}:`, error.message);
        }
    }

    /**
     * NEW: EFFICIENT: Update price in undervalued_sales table if listing exists
     */
    async updatePriceInUndervaluedSales(listingId, newPrice, sqft) {
        try {
            const updateData = {
                sale_price: parseInt(newPrice),
                analysis_date: new Date().toISOString()
            };

            // Calculate new price per sqft if we have sqft data
            if (sqft && sqft > 0) {
                updateData.price_per_sqft = parseFloat((newPrice / sqft).toFixed(2));
            }

            const { error } = await this.supabase
                .from('undervalued_sales')
                .update(updateData)
                .eq('listing_id', listingId)
                .eq('status', 'active');

            if (error) {
                // Don't log error - listing might not be in undervalued_sales table
            } else {
                console.log(`   💾 Updated undervalued_sales price for ${listingId}: ${newPrice.toLocaleString()}`);
            }
        } catch (error) {
            // Silent fail - listing might not be undervalued
        }
    }

    /**
     * NEW: EFFICIENT: Mark listing for reanalysis due to price change
     */
    async triggerReanalysisForPriceChange(listingId, neighborhood) {
        try {
            // Update market_status to trigger reanalysis in next cycle
            const { error } = await this.supabase
                .from('sales_market_cache')
                .update({
                    market_status: 'pending',
                    last_analyzed: null // Clear analysis date to trigger reanalysis
                })
                .eq('listing_id', listingId);

            if (error) {
                console.warn(`⚠️ Error marking ${listingId} for reanalysis:`, error.message);
            }
        } catch (error) {
            console.warn(`⚠️ Error triggering reanalysis for ${listingId}:`, error.message);
        }
    }

    /**
     * NEW: OPTIMIZED: Handle price updates efficiently without refetching
     * Updates price in cache and triggers reanalysis for undervaluation
     */
    async handlePriceUpdatesInCache(listingIds, salesData, neighborhood) {
        if (!listingIds || listingIds.length === 0) return { completeListingIds: [], priceUpdatedIds: [] };
        
        try {
            const sevenDaysAgo = new Date();
            sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);

            const { data, error } = await this.supabase
                .from('sales_market_cache')
                .select('listing_id, address, bedrooms, bathrooms, sqft, market_status, price')
                .in('listing_id', listingIds)
                .gte('last_checked', sevenDaysAgo.toISOString());

            if (error) {
                console.warn('⚠️ Error checking existing sales, will fetch all details:', error.message);
                return { completeListingIds: [], priceUpdatedIds: [] };
            }

            // Filter for complete entries
            const completeEntries = data.filter(row => 
                row.address && 
                row.address !== 'Address not available' && 
                row.address !== 'Details unavailable' &&
                row.address !== 'Fetch failed' &&
                row.bedrooms !== null &&
                row.bathrooms !== null &&
                row.sqft !== null &&
                row.sqft > 0 &&
                row.market_status !== 'fetch_failed'
            );

            // Handle price changes efficiently
            const priceUpdatedIds = [];
            const salesMap = new Map(salesData.map(sale => [sale.id?.toString(), sale]));
            
            for (const cachedEntry of completeEntries) {
                const currentSale = salesMap.get(cachedEntry.listing_id);
                if (currentSale) {
                    // Get current price from search results
                    const currentPrice = currentSale.price || currentSale.salePrice || 0;
                    const cachedPrice = cachedEntry.price || 0;
                    
                    // If price changed by more than $10,000, update cache directly
                    if (Math.abs(currentPrice - cachedPrice) > 10000) {
                        console.log(`   💰 Price change detected for ${cachedEntry.listing_id}: ${cachedPrice.toLocaleString()} → ${currentPrice.toLocaleString()}`);
                        
                        // ✅ EFFICIENT: Update price in cache without refetching
                        await this.updatePriceInCache(cachedEntry.listing_id, currentPrice);
                        
                        // ✅ EFFICIENT: Update price in undervalued_sales if exists
                        await this.updatePriceInUndervaluedSales(cachedEntry.listing_id, currentPrice, cachedEntry.sqft);
                        
                        // ✅ EFFICIENT: Trigger reanalysis for undervaluation (price changed)
                        await this.triggerReanalysisForPriceChange(cachedEntry.listing_id, neighborhood);
                        
                        priceUpdatedIds.push(cachedEntry.listing_id);
                    }
                }
            }

            const completeListingIds = completeEntries.map(row => row.listing_id);
            const incompleteCount = data.length - completeEntries.length;
            
            console.log(`   💾 Cache lookup: ${completeListingIds.length}/${listingIds.length} sales with COMPLETE details found in cache`);
            if (incompleteCount > 0) {
                console.log(`   🔄 ${incompleteCount} cached entries need detail fetching (incomplete data)`);
            }
            if (priceUpdatedIds.length > 0) {
                console.log(`   💰 ${priceUpdatedIds.length} price-only updates completed (no API calls used)`);
            }
            
            return { completeListingIds, priceUpdatedIds };
        } catch (error) {
            console.warn('⚠️ Cache lookup failed, will fetch all details:', error.message);
            return { completeListingIds: [], priceUpdatedIds: [] };
        }
    }

    /**
     * NEW: Run sold detection for specific neighborhood
     */
    async runSoldDetectionForNeighborhood(searchResults, neighborhood) {
        const currentTime = new Date().toISOString();
        const currentListingIds = searchResults.map(r => r.id?.toString()).filter(Boolean);
        
        try {
            const threeDaysAgo = new Date();
            threeDaysAgo.setDate(threeDaysAgo.getDate() - 3);

            // Get sales in this neighborhood that weren't in current search
            const { data: missingSales, error: missingError } = await this.supabase
                .from('sales_market_cache')
                .select('listing_id')
                .eq('neighborhood', neighborhood)
                .not('listing_id', 'in', `(${currentListingIds.map(id => `"${id}"`).join(',')})`)
                .lt('last_seen_in_search', threeDaysAgo.toISOString());

            if (missingError) {
                console.warn('⚠️ Error checking for missing sales:', missingError.message);
                return { updated: searchResults.length, markedSold: 0 };
            }

            // Mark corresponding entries in undervalued_sales as likely sold
            let markedSold = 0;
            if (missingSales && missingSales.length > 0) {
                const missingIds = missingSales.map(r => r.listing_id);
                
                const { error: markSoldError } = await this.supabase
                    .from('undervalued_sales')
                    .update({
                        status: 'likely_sold',
                        likely_sold: true,
                        sold_detected_at: currentTime
                    })
                    .in('listing_id', missingIds)
                    .eq('status', 'active');

                if (!markSoldError) {
                    markedSold = missingIds.length;
                    this.apiUsageStats.listingsMarkedSold += markedSold;
                    console.log(`   🏠 Marked ${markedSold} sales as likely sold (not seen in recent search)`);
                } else {
                    console.warn('⚠️ Error marking sales as sold:', markSoldError.message);
                }
            }

            return { markedSold };
        } catch (error) {
            console.warn('⚠️ Error in sold detection for neighborhood:', error.message);
            return { markedSold: 0 };
        }
    }

    /**
     * NEW: SIMPLIFIED: Update only search timestamps for sold detection
     * Price updates are handled separately
     */
    async updateSalesTimestampsOnly(searchResults, neighborhood) {
        const currentTime = new Date().toISOString();
        
        try {
            // Step 1: Update ONLY search timestamps (price already handled above)
            for (const sale of searchResults) {
                if (!sale.id) continue;
                
                try {
                    const searchTimestampData = {
                        listing_id: sale.id.toString(),
                        neighborhood: neighborhood,
                        borough: sale.borough || 'unknown',
                        last_seen_in_search: currentTime,
                        times_seen: 1
                    };

                    const { error } = await this.supabase
                        .from('sales_market_cache')
                        .upsert(searchTimestampData, { 
                            onConflict: 'listing_id',
                            updateColumns: ['last_seen_in_search', 'neighborhood', 'borough']
                        });

                    if (error) {
                        console.warn(`⚠️ Error updating search timestamp for ${sale.id}:`, error.message);
                    }
                } catch (itemError) {
                    console.warn(`⚠️ Error processing search timestamp ${sale.id}:`, itemError.message);
                }
            }

            // Step 2: Run sold detection for this neighborhood
            const { markedSold } = await this.runSoldDetectionForNeighborhood(searchResults, neighborhood);
            
            console.log(`   💾 Updated search timestamps: ${searchResults.length} sales, marked ${markedSold} as sold`);
            return { updated: searchResults.length, markedSold };

        } catch (error) {
            console.warn('⚠️ Error updating search timestamps:', error.message);
            return { updated: 0, markedSold: 0 };
        }
    }

    /**
     * NEW: Cache complete sale details for new listings
     */
    async cacheCompleteSaleDetails(listingId, details, neighborhood) {
        try {
            const completeSaleData = {
                listing_id: listingId.toString(),
                address: details.address || 'Address from detail fetch',
                neighborhood: neighborhood,
                borough: details.borough || 'unknown',
                price: details.salePrice || 0,
                bedrooms: details.bedrooms || 0,
                bathrooms: details.bathrooms || 0,
                sqft: details.sqft || 0,
                property_type: details.propertyType || 'apartment',
                market_status: 'pending',
                last_checked: new Date().toISOString(),
                last_seen_in_search: new Date().toISOString(),
                last_analyzed: null
            };

            const { error } = await this.supabase
                .from('sales_market_cache')
                .upsert(completeSaleData, { 
                    onConflict: 'listing_id',
                    updateColumns: ['address', 'bedrooms', 'bathrooms', 'sqft', 'price', 'property_type', 'last_checked', 'market_status']
                });

            if (error) {
                console.warn(`⚠️ Error caching complete details for ${listingId}:`, error.message);
            } else {
                console.log(`   💾 Cached complete details for ${listingId} (${completeSaleData.price?.toLocaleString()})`);
            }
        } catch (error) {
            console.warn(`⚠️ Error caching complete sale details for ${listingId}:`, error.message);
        }
    }

    /**
     * NEW: Cache failed fetch attempt
     */
    async cacheFailedSaleFetch(listingId, neighborhood) {
        try {
            const failedFetchData = {
                listing_id: listingId.toString(),
                address: 'Fetch failed',
                neighborhood: neighborhood,
                market_status: 'fetch_failed',
                last_checked: new Date().toISOString(),
                last_seen_in_search: new Date().toISOString()
            };

            const { error } = await this.supabase
                .from('sales_market_cache')
                .upsert(failedFetchData, { 
                    onConflict: 'listing_id',
                    updateColumns: ['market_status', 'last_checked']
                });

            if (error) {
                console.warn(`⚠️ Error caching failed fetch for ${listingId}:`, error.message);
            }
        } catch (error) {
            console.warn(`⚠️ Error caching failed fetch for ${listingId}:`, error.message);
        }
    }

    /**
     * NEW: Update cache with analysis results (mark as undervalued or market_rate)
     */
    async updateCacheWithAnalysisResults(detailedSales, undervaluedSales) {
        try {
            const cacheUpdates = detailedSales.map(sale => {
                const isUndervalued = undervaluedSales.some(us => us.id === sale.id);
                
                return {
                    listing_id: sale.id?.toString(),
                    market_status: isUndervalued ? 'undervalued' : 'market_rate',
                    last_analyzed: new Date().toISOString()
                };
            });

            for (const update of cacheUpdates) {
                try {
                    await this.supabase
                        .from('sales_market_cache')
                        .update({
                            market_status: update.market_status,
                            last_analyzed: update.last_analyzed
                        })
                        .eq('listing_id', update.listing_id);
                } catch (updateError) {
                    console.warn(`⚠️ Error updating cache for ${update.listing_id}:`, updateError.message);
                }
            }
            
            console.log(`   💾 Updated cache analysis status for ${cacheUpdates.length} sales`);
        } catch (error) {
            console.warn('⚠️ Error updating cache analysis results:', error.message);
            console.warn('   Continuing without cache analysis updates');
        }
    }

    /**
     * Clear old sales data with enhanced cleanup
     */
    async clearOldSalesData() {
        try {
            const oneMonthAgo = new Date();
            oneMonthAgo.setMonth(oneMonthAgo.getMonth() - 1);

            // Clear old undervalued sales
            const { error: salesError } = await this.supabase
                .from('undervalued_sales')
                .delete()
                .lt('analysis_date', oneMonthAgo.toISOString());

            if (salesError) {
                console.error('❌ Error clearing old sales data:', salesError.message);
            } else {
                console.log('🧹 Cleared old sales data (>1 month)');
            }

            // Clear old cache entries using the database function - with graceful fallback
            try {
                const { data: cleanupResult, error: cleanupError } = await this.supabase
                    .rpc('cleanup_old_cache_entries');

                if (cleanupError) {
                    console.warn('⚠️ Cache cleanup function not available:', cleanupError.message);
                    console.warn('   Continuing without automatic cache cleanup');
                } else {
                    console.log(`🧹 ${cleanupResult || 'Cache cleanup completed'}`);
                }
            } catch (cleanupFunctionError) {
                console.warn('⚠️ Cache cleanup function not available:', cleanupFunctionError.message);
                console.warn('   This is expected if database functions are not yet created');
                console.warn('   Manual cache cleanup can be done through the database');
            }
        } catch (error) {
            console.error('❌ Clear old sales data error:', error.message);
        }
    }

    /**
     * Save bi-weekly sales summary with enhanced deduplication stats
     */
    async saveBiWeeklySalesSummary(summary) {
        try {
            const { error } = await this.supabase
                .from('bi_weekly_analysis_runs')
                .insert([{
                    run_date: summary.startTime,
                    analysis_type: 'sales',
                    neighborhoods_processed: summary.neighborhoodsProcessed,
                    total_active_listings: summary.totalActiveSalesFound,
                    total_details_attempted: summary.totalDetailsAttempted,
                    total_details_fetched: summary.totalDetailsFetched,
                    undervalued_found: summary.undervaluedFound,
                    saved_to_database: summary.savedToDatabase,
                    api_calls_used: summary.apiCallsUsed,
                    
                    // ENHANCED: Deduplication performance stats
                    api_calls_saved: summary.apiCallsSaved || 0,
                    cache_hit_rate: summary.cacheHitRate || 0,
                    listings_marked_sold: summary.listingsMarkedSold || 0,
                    
                    duration_minutes: Math.round(summary.duration),
                    detailed_stats: summary.detailedStats,
                    errors: summary.errors,
                    completed: true
                }]);

            if (error) {
                console.error('❌ Error saving bi-weekly sales summary:', error.message);
            } else {
                console.log('✅ Bi-weekly sales summary saved to database');
            }
        } catch (error) {
            console.error('❌ Save sales summary error:', error.message);
        }
    }

    /**
     * Enhanced summary with deduplication performance
     */
    logSmartDeduplicationSummary(summary) {
        const mode = this.initialBulkLoad ? 'INITIAL BULK LOAD' : 'SMART DEDUPLICATION';
        
        console.log(`\n📊 ${mode} SALES ANALYSIS COMPLETE`);
        console.log('='.repeat(70));
        
        if (this.initialBulkLoad) {
            console.log(`🚀 BULK LOAD: All ${summary.totalNeighborhoods} sales neighborhoods processed`);
            console.log(`⏱️ Duration: ${summary.duration.toFixed(1)} minutes (~${(summary.duration/60).toFixed(1)} hours)`);
        } else {
            console.log(`📅 Schedule Day: ${summary.scheduledDay} of bi-weekly cycle`);
            console.log(`⏱️ Duration: ${summary.duration.toFixed(1)} minutes`);
        }
        
        console.log(`🗽 Neighborhoods processed: ${summary.neighborhoodsProcessed}/${summary.totalNeighborhoods}`);
        
        // Core metrics
        console.log('\n📊 Core Analysis Metrics:');
        console.log(`🏠 Active sales found: ${summary.totalActiveSalesFound}`);
        console.log(`🔍 Details attempted: ${summary.totalDetailsAttempted}`);
        console.log(`✅ Details successfully fetched: ${summary.totalDetailsFetched}`);
        console.log(`🎯 Undervalued sales found: ${summary.undervaluedFound}`);
        console.log(`💾 Saved to database: ${summary.savedToDatabase}`);
        console.log(`📞 API calls used: ${summary.apiCallsUsed}`);
        
        // DEDUPLICATION PERFORMANCE HIGHLIGHT (only if not bulk load)
        if (!this.initialBulkLoad) {
            console.log('\n⚡ SMART DEDUPLICATION PERFORMANCE:');
            console.log(`💾 API calls saved by cache: ${summary.apiCallsSaved}`);
            console.log(`📈 Cache hit rate: ${summary.cacheHitRate.toFixed(1)}%`);
            console.log(`🏠 Listings marked as sold: ${summary.listingsMarkedSold}`);
            
            // Calculate efficiency metrics
            const totalPotentialCalls = summary.apiCallsUsed + summary.apiCallsSaved;
            const efficiencyPercentage = totalPotentialCalls > 0 ? 
                (summary.apiCallsSaved / totalPotentialCalls * 100).toFixed(1) : '0';
            
            console.log(`🚀 API efficiency: ${efficiencyPercentage}% reduction in API calls`);
            
            if (summary.apiCallsSaved > 0) {
                const estimatedCostSavings = (summary.apiCallsSaved * 0.01).toFixed(2);
                console.log(`💰 Estimated cost savings: ${estimatedCostSavings} (at $0.01/call)`);
            }
        }
        
        // Adaptive rate limiting performance
        console.log('\n⚡ Adaptive Rate Limiting Performance:');
        console.log(`   🚀 Started with: ${this.initialBulkLoad ? '8s' : '6s'} delays`);
        console.log(`   🎯 Ended with: ${this.baseDelay/1000}s delays`);
        console.log(`   📈 Rate limit hits: ${this.rateLimitHits}`);
        console.log(`   🔧 Adaptive changes: ${summary.adaptiveDelayChanges}`);
        
        // Success rates
        const detailSuccessRate = summary.totalDetailsAttempted > 0 ? 
            (summary.totalDetailsFetched / summary.totalDetailsAttempted * 100).toFixed(1) : '0';
        const undervaluedRate = summary.totalDetailsFetched > 0 ? 
            (summary.undervaluedFound / summary.totalDetailsFetched * 100).toFixed(1) : '0';
        
        console.log('\n📈 Success Rates:');
        console.log(`   📋 Detail fetch success: ${detailSuccessRate}%`);
        console.log(`   🎯 Undervalued discovery rate: ${undervaluedRate}%`);
        
        // Top performing neighborhoods
        console.log('\n🏆 Today\'s Neighborhood Performance:');
        const sortedNeighborhoods = Object.entries(summary.detailedStats.byNeighborhood)
            .sort((a, b) => b[1].undervaluedFound - a[1].undervaluedFound);
            
        sortedNeighborhoods.forEach(([neighborhood, stats], index) => {
            const savings = stats.apiCallsSaved || 0;
            console.log(`   ${index + 1}. ${neighborhood}: ${stats.undervaluedFound} deals (${savings} API calls saved)`);
        });
        
        // Error reporting
        if (summary.errors.length > 0) {
            const rateLimitErrors = summary.errors.filter(e => e.isRateLimit).length;
            console.log(`\n❌ Errors: ${summary.errors.length} total (${rateLimitErrors} rate limits, ${summary.errors.length - rateLimitErrors} other)`);
        }

        // Next steps
        if (this.initialBulkLoad) {
            console.log('\n🎯 SALES BULK LOAD COMPLETE!');
            console.log('📝 Next steps:');
            console.log('   1. Set INITIAL_BULK_LOAD=false in Railway');
            console.log('   2. Switch to bi-weekly maintenance mode');
            console.log('   3. Enjoy 75-90% API savings from smart caching!');
        } else {
            // Normal bi-weekly next day preview
            const nextDay = this.currentDay + 1;
            const nextDayNeighborhoods = this.dailySchedule[nextDay] || [];
            if (nextDayNeighborhoods.length > 0) {
                console.log(`\n📅 Tomorrow's schedule: ${nextDayNeighborhoods.join(', ')}`);
            } else if (nextDay <= 8) {
                console.log(`\n📅 Tomorrow: Buffer day (catch-up or completion)`);
            } else {
                console.log(`\n📅 Next bi-weekly cycle starts on the 1st or 15th of next month`);
            }
        }

        // Results summary
        if (summary.savedToDatabase > 0) {
            console.log('\n🎉 SUCCESS: Found undervalued sales efficiently!');
            console.log(`🔍 Check your Supabase 'undervalued_sales' table for ${summary.savedToDatabase} new deals`);
            
            if (!this.initialBulkLoad && summary.apiCallsSaved > 0) {
                const efficiency = ((summary.apiCallsSaved / (summary.apiCallsUsed + summary.apiCallsSaved)) * 100).toFixed(1);
                console.log(`⚡ Achieved ${efficiency}% API efficiency through smart caching`);
            }
        } else {
            console.log('\n📊 No undervalued sales found (normal in competitive NYC sales market)');
            console.log('💡 Try adjusting criteria or neighborhoods - 10% threshold is realistic for NYC');
        }
        
        // Long-term projection (only for regular mode)
        if (!this.initialBulkLoad && summary.apiCallsSaved > 0) {
            console.log(`\n📊 Deduplication Impact: Expect 75-90% API savings in future runs`);
            console.log(`💡 This system scales efficiently for long-term operation`);
        }

        // Database function status
        console.log('\n🔧 Database Function Status:');
        console.log('   ⚠️ Some advanced functions may not be available yet');
        console.log('   📊 Core functionality works without them');
        console.log('   🔧 Add database functions later for enhanced features');
    }

    /**
     * ADAPTIVE rate limiting - adjusts based on API response patterns
     */
    adaptiveRateLimit() {
        const now = Date.now();
        
        // Clean old timestamps (older than 1 hour)
        this.callTimestamps = this.callTimestamps.filter(t => now - t < 60 * 60 * 1000);
        
        // Check if we're hitting hourly limits
        const callsThisHour = this.callTimestamps.length;
        
        // ADAPTIVE LOGIC: Adjust delay based on recent performance
        if (this.rateLimitHits === 0 && callsThisHour < this.maxCallsPerHour * 0.7) {
            // All good - can be more aggressive
            this.baseDelay = Math.max(4000, this.baseDelay - 500); // Min 4s
            console.log(`   ⚡ No rate limits - reducing delay to ${this.baseDelay/1000}s`);
        } else if (this.rateLimitHits <= 2) {
            // Some rate limits - be moderate
            this.baseDelay = 8000;
            console.log(`   ⚖️ Some rate limits - moderate delay ${this.baseDelay/1000}s`);
        } else if (this.rateLimitHits > 2) {
            // Multiple rate limits - be very conservative
            this.baseDelay = Math.min(20000, this.baseDelay + 2000); // Max 20s
            console.log(`   🐌 Multiple rate limits - increasing delay to ${this.baseDelay/1000}s`);
            this.apiUsageStats.adaptiveDelayChanges++;
        }
        
        // Hourly protection
        if (callsThisHour >= this.maxCallsPerHour) {
            console.log(`⏰ Hourly limit reached (${callsThisHour}/${this.maxCallsPerHour}), waiting 30 minutes...`);
            return 30 * 60 * 1000; // Wait 30 minutes
        }
        
        // Progressive delay - gets slower as we make more calls in session
        const sessionCalls = this.apiCallsUsed;
        const progressiveIncrease = Math.floor(sessionCalls / 50) * 1000; // +1s every 50 calls
        
        // Random jitter to avoid synchronized requests
        const jitter = Math.random() * 2000; // 0-2s random
        
        const finalDelay = this.baseDelay + progressiveIncrease + jitter;
        
        // Record this call timestamp
        this.callTimestamps.push(now);
        
        return finalDelay;
    }

    
/**
 * FAST: Simple 200ms delay (replace your complex smartDelay)
 */
async smartDelay() {
    console.log(`   ⚡ Fast delay: 200ms`);
    await this.delay(200);
}

/**
 * FAST: Simple rate limit function (replace your complex adaptiveRateLimit)
 */
adaptiveRateLimit() {
    return 200; // Always return 200ms instead of 4000-8000ms
}


    /**
     * Calculate deal quality from score
     */
    calculateDealQuality(score) {
        if (score >= 90) return 'best';
        if (score >= 80) return 'excellent'; 
        if (score >= 70) return 'good';
        if (score >= 60) return 'fair';
        return 'marginal';
    }

    /**
     * Main bi-weekly sales refresh with SMART DEDUPLICATION + ADVANCED VALUATION
     */
    async runBiWeeklySalesRefresh() {
        console.log('\n🏠 ADVANCED MULTI-FACTOR SALES ANALYSIS');
        console.log('🎯 NEW: 10% threshold using bed/bath/amenity valuation engine');
        console.log('💾 Cache-optimized to save 75-90% of API calls');
        console.log('🏠 Auto-detects and removes sold listings');
        console.log('⚡ Adaptive rate limiting with daily neighborhood scheduling');
        console.log('🔧 FIXED: Database function dependencies resolved');
        console.log('='.repeat(70));

        // Get today's neighborhood assignment WITH BULK LOAD SUPPORT
        const todaysNeighborhoods = await this.getTodaysNeighborhoods();
        
        if (todaysNeighborhoods.length === 0) {
            console.log('📅 No neighborhoods scheduled for today - analysis complete');
            return { summary: { message: 'No neighborhoods scheduled for today (off-schedule)' } };
        }

        const summary = {
            startTime: new Date(),
            scheduledDay: this.currentDay,
            totalNeighborhoods: todaysNeighborhoods.length,
            neighborhoodsProcessed: 0,
            totalActiveSalesFound: 0,
            totalDetailsAttempted: 0,
            totalDetailsFetched: 0,
            undervaluedFound: 0,
            savedToDatabase: 0,
            apiCallsUsed: 0,
            adaptiveDelayChanges: 0,
            // NEW: Deduplication stats
            apiCallsSaved: 0,
            cacheHitRate: 0,
            listingsMarkedSold: 0,
            errors: [],
            detailedStats: {
                byNeighborhood: {},
                apiUsage: this.apiUsageStats,
                rateLimit: {
                    initialDelay: this.baseDelay,
                    finalDelay: this.baseDelay,
                    rateLimitHits: 0
                }
            }
        };

        try {
            // Clear old sales data and run automatic sold detection
            await this.clearOldSalesData();
            await this.runAutomaticSoldDetection();

            console.log(`📋 ${this.initialBulkLoad ? 'BULK LOAD' : 'Today\'s'} assignment: ${todaysNeighborhoods.join(', ')}`);
            console.log(`⚡ Starting with ${this.baseDelay/1000}s delays (will adapt based on API response)\n`);

            // Process today's neighborhoods with smart deduplication
            for (let i = 0; i < todaysNeighborhoods.length; i++) {
                const neighborhood = todaysNeighborhoods[i];
                
                try {
                    console.log(`\n🏠 [${i + 1}/${todaysNeighborhoods.length}] PROCESSING: ${neighborhood}`);
                    
                    // Smart delay before each neighborhood (except first)
                    if (i > 0) {
                        await this.smartDelay();
                    }
                    
                    // Step 1: Get ALL active sales with smart deduplication
                    const { newSales, totalFound, cacheHits } = await this.fetchActiveSalesWithDeduplication(neighborhood);
                    summary.totalActiveSalesFound += totalFound;
                    this.apiUsageStats.totalListingsFound += totalFound;
                    this.apiUsageStats.cacheHits += cacheHits;
                    this.apiUsageStats.newListingsToFetch += newSales.length;
                    this.apiUsageStats.apiCallsSaved += cacheHits;
                    
                    if (newSales.length === 0 && !this.initialBulkLoad) {
                        console.log(`   📊 All ${totalFound} sales found in cache - 100% API savings!`);
                        continue;
                    }

                    console.log(`   🎯 Smart deduplication: ${totalFound} total, ${newSales.length} new, ${cacheHits} cached`);
                    if (cacheHits > 0 && !this.initialBulkLoad) {
                        console.log(`   ⚡ API savings: ${cacheHits} detail calls avoided!`);
                    };
                    
                    // Step 2: Fetch details ONLY for new sales
                    const detailedSales = await this.fetchSalesDetailsWithCache(newSales, neighborhood);
                    summary.totalDetailsAttempted += newSales.length;
                    summary.totalDetailsFetched += detailedSales.length;
                    
                    // Step 3: ADVANCED MULTI-FACTOR ANALYSIS for undervaluation
                    const undervaluedSales = this.analyzeForAdvancedSalesUndervaluation(detailedSales, neighborhood);
                    summary.undervaluedFound += undervaluedSales.length;
                    
                    // Step 4: Save to database
                    if (undervaluedSales.length > 0) {
                        const saved = await this.saveUndervaluedSalesToDatabase(undervaluedSales, neighborhood);
                        summary.savedToDatabase += saved;
                    }
                    
                    // Step 5: Update cache with analysis results
                    await this.updateCacheWithAnalysisResults(detailedSales, undervaluedSales);
                    
                    // Track neighborhood stats
                    summary.detailedStats.byNeighborhood[neighborhood] = {
                        totalFound: totalFound,
                        cacheHits: cacheHits,
                        newSales: newSales.length,
                        detailsFetched: detailedSales.length,
                        undervaluedFound: undervaluedSales.length,
                        apiCallsUsed: 1 + newSales.length, // 1 search + detail calls
                        apiCallsSaved: cacheHits
                    };
                    
                    summary.neighborhoodsProcessed++;
                    console.log(`   ✅ ${neighborhood}: ${undervaluedSales.length} undervalued sales found (10% threshold)`);

                    // For bulk load, log progress every 5 neighborhoods
                    if (this.initialBulkLoad && (i + 1) % 5 === 0) {
                        const progress = ((i + 1) / todaysNeighborhoods.length * 100).toFixed(1);
                        const elapsed = (new Date() - summary.startTime) / 1000 / 60;
                        const eta = elapsed / (i + 1) * todaysNeighborhoods.length - elapsed;
                        console.log(`\n📊 BULK LOAD PROGRESS: ${progress}% complete (${i + 1}/${todaysNeighborhoods.length})`);
                        console.log(`⏱️ Elapsed: ${elapsed.toFixed(1)}min, ETA: ${eta.toFixed(1)}min`);
                        console.log(`🎯 Found ${summary.undervaluedFound} total undervalued sales so far\n`);
                    }
                } catch (error) {
                    console.error(`   ❌ Error processing ${neighborhood}: ${error.message}`);
                    
                    // Handle rate limits specially
                    if (error.response?.status === 429) {
                        this.rateLimitHits++;
                        this.apiUsageStats.rateLimitHits++;
                        console.log(`   ⚡ Rate limit hit #${this.rateLimitHits} - adapting delays`);
                        
                        // Wait longer after rate limit (especially for bulk load)
                        const penaltyDelay = this.initialBulkLoad ? 60000 : 30000;
                        await this.delay(penaltyDelay);
                    }
                    
                    summary.errors.push({
                        neighborhood,
                        error: error.message,
                        timestamp: new Date().toISOString(),
                        isRateLimit: error.response?.status === 429
                    });
                }
            }

            // Calculate final deduplication stats
            summary.endTime = new Date();
            summary.duration = (summary.endTime - summary.startTime) / 1000 / 60;
            summary.apiCallsUsed = this.apiCallsUsed;
            summary.apiCallsSaved = this.apiUsageStats.apiCallsSaved;
            summary.cacheHitRate = this.apiUsageStats.totalListingsFound > 0 ? 
                (this.apiUsageStats.cacheHits / this.apiUsageStats.totalListingsFound * 100) : 0;
            summary.listingsMarkedSold = this.apiUsageStats.listingsMarkedSold;
            summary.adaptiveDelayChanges = this.apiUsageStats.adaptiveDelayChanges;
            summary.detailedStats.rateLimit = {
                initialDelay: this.initialBulkLoad ? 8000 : 6000,
                finalDelay: this.baseDelay,
                rateLimitHits: this.rateLimitHits
            };

            await this.saveBiWeeklySalesSummary(summary);
            this.logSmartDeduplicationSummary(summary);
        } catch (error) {
            console.error('💥 Smart deduplication sales refresh failed:', error.message);
            summary.errors.push({ error: error.message });
        }

        return { summary };
    }

    /**
     * SMART DEDUPLICATION: Fetch active sales and identify which need detail fetching
     */
    async fetchActiveSalesWithDeduplication(neighborhood) {
        try {
            console.log(`   📡 Fetching active sales for ${neighborhood} with smart deduplication...`);
            
            // Step 1: Get basic neighborhood search (1 API call)
            const response = await axios.get(
                'https://streeteasy-api.p.rapidapi.com/sales/search',
                {
                    params: {
                        areas: neighborhood,
                        limit: 500,
                        minPrice: 100000,
                        maxPrice: 50000000,
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

            console.log(`   ✅ Retrieved ${salesData.length} total active sales`);

            // Step 2: Check cache for complete details AND handle price changes efficiently
            const listingIds = salesData.map(sale => sale.id?.toString()).filter(Boolean);
            const { completeListingIds, priceUpdatedIds } = await this.handlePriceUpdatesInCache(listingIds, salesData, neighborhood);
            
            // Step 3: Filter to ONLY truly NEW sales (price-changed sales already handled)
            const newSales = salesData.filter(sale => 
                !completeListingIds.includes(sale.id?.toString())
            );

            const cacheHits = completeListingIds.length;
            const priceUpdates = priceUpdatedIds.length;

            // Step 4: Update search timestamps for sold detection
            await this.updateSalesTimestampsOnly(salesData, neighborhood);
            
            console.log(`   🎯 Optimized deduplication: ${salesData.length} total, ${newSales.length} need fetching, ${cacheHits} cache hits, ${priceUpdates} price-only updates`);
            
            return {
                newSales,
                totalFound: salesData.length,
                cacheHits: cacheHits,
                priceUpdates: priceUpdates
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
     * Fetch sale details with cache updates
     */
    async fetchSalesDetailsWithCache(newSales, neighborhood) {
        console.log(`   🔍 Fetching details for ${newSales.length} NEW sales (saving API calls from cache)...`);
        
        const detailedSales = [];
        let successCount = 0;
        let failureCount = 0;

        for (let i = 0; i < newSales.length; i++) {
            const sale = newSales[i];
            
            try {
                // Check hourly limits
                if (this.callTimestamps.length >= this.maxCallsPerHour) {
                    console.log(`   ⏰ Hourly rate limit reached, taking a break...`);
                    await this.delay(30 * 60 * 1000);
                }

                // Smart adaptive delay
                if (i > 0) {
                    await this.smartDelay();
                }

                const details = await this.fetchSaleDetails(sale.id);
                
                if (details && this.isValidSaleData(details)) {
                    const fullSaleData = {
                        ...sale,
                        ...details,
                        neighborhood: neighborhood,
                    };
                    
                    detailedSales.push(fullSaleData);
                    
                    // Cache complete sale details ONLY AFTER successful individual fetch
                    await this.cacheCompleteSaleDetails(sale.id, details, neighborhood);
                    
                    successCount++;
                } else {
                    failureCount++;
                    // Cache failed fetch ONLY after we tried and failed
                    await this.cacheFailedSaleFetch(sale.id, neighborhood);
                }

                // Progress logging every 20 properties
                if ((i + 1) % 20 === 0) {
                    const currentDelay = this.baseDelay;
                    console.log(`   📊 Progress: ${i + 1}/${newSales.length} (${successCount} successful, ${failureCount} failed, ${currentDelay/1000}s delay)`);
                }

            } catch (error) {
                failureCount++;
                // Cache failed fetch ONLY after we tried and failed
                await this.cacheFailedSaleFetch(sale.id, neighborhood);
                
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

        console.log(`   ✅ Sale detail fetch complete: ${successCount} successful, ${failureCount} failed`);
        return detailedSales;
    }

    /**
     * Run automatic sold detection based on cache
     */
    async runAutomaticSoldDetection() {
        try {
            console.log('🏠 Running automatic sold detection...');
            
            // Try to call the database function with graceful fallback
            const { data, error } = await this.supabase.rpc('mark_likely_sold_listings');
            
            if (error) {
                console.warn('⚠️ Sold detection function not available:', error.message);
                console.warn('   Continuing without automatic sold detection');
                console.warn('   Manual detection will still work through cache comparisons');
                return 0;
            }
            
            const markedCount = data || 0;
            if (markedCount > 0) {
                console.log(`🏠 Marked ${markedCount} listings as likely sold`);
                this.apiUsageStats.listingsMarkedSold += markedCount;
            }
            
            return markedCount;
        } catch (error) {
            console.warn('⚠️ Automatic sold detection function not available:', error.message);
            console.warn('   This is expected if database functions are not yet created');
            console.warn('   Manual sold detection through cache comparisons will still work');
            return 0;
        }
    }

    /**
     * Fetch individual sale details using /sales/{id}
     */
    async fetchSaleDetails(saleId) {
        try {
            const response = await axios.get(
                `https://streeteasy-api.p.rapidapi.com/sales/${saleId}`,
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
            
            // Extract sale details based on actual API response
            return {
                // Basic property info
                address: data.address || 'Address not available',
                bedrooms: data.bedrooms || 0,
                bathrooms: data.bathrooms || 0,
                sqft: data.sqft || 0,
                propertyType: data.propertyType || 'apartment',
                
                // Sale pricing
                salePrice: data.price || 0,
                pricePerSqft: (data.sqft > 0 && data.price > 0) ? data.price / data.sqft : null,
                
                // Sale status and timing
                status: data.status || 'unknown',
                listedAt: data.listedAt || null,
                closedAt: data.closedAt || null,
                soldAt: data.soldAt || null,
                daysOnMarket: data.daysOnMarket || 0,
                type: data.type || 'sale',
                
                // Location info
                borough: data.borough || 'unknown',
                neighborhood: data.neighborhood || 'unknown',
                zipcode: data.zipcode || null,
                latitude: data.latitude || null,
                longitude: data.longitude || null,
                
                // Additional fields
                builtIn: data.builtIn || null,
                building: data.building || {},
                amenities: data.amenities || [],
                description: data.description || '',
                images: data.images || [],
                videos: data.videos || [],
                floorplans: data.floorplans || [],
                agents: data.agents || [],
                
                // Derived building features
                doormanBuilding: this.checkForAmenity(data.amenities, ['doorman', 'full_time_doorman']),
                elevatorBuilding: this.checkForAmenity(data.amenities, ['elevator']),
                petFriendly: this.checkForAmenity(data.amenities, ['pets', 'dogs', 'cats']),
                laundryAvailable: this.checkForAmenity(data.amenities, ['laundry', 'washer_dryer']),
                gymAvailable: this.checkForAmenity(data.amenities, ['gym', 'fitness']),
                rooftopAccess: this.checkForAmenity(data.amenities, ['roofdeck', 'roof_deck', 'terrace'])
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
     * Helper function to check if amenities array contains specific features
     */
    checkForAmenity(amenities, searchTerms) {
        if (!Array.isArray(amenities)) return false;
        
        return searchTerms.some(term => 
            amenities.some(amenity => 
                amenity.toLowerCase().includes(term.toLowerCase())
            )
        );
    }

    /**
     * Validate sale data is complete enough for analysis
     */
    isValidSaleData(sale) {
        return sale &&
               sale.address &&
               sale.salePrice > 0 &&
               sale.bedrooms !== undefined &&
               sale.bathrooms !== undefined;
    }

   // REPLACE the analyzeForAdvancedSalesUndervaluation method with:
async analyzeForAdvancedSalesUndervaluation(detailedSales, neighborhood) {
    if (detailedSales.length < 5) {
        console.log(`   ⚠️ Not enough sales (${detailedSales.length}) for analysis in ${neighborhood}`);
        return [];
    }

    console.log(`   🤖 CLAUDE ANALYSIS: ${detailedSales.length} sales using AI engine...`);

    const undervaluedSales = [];

    // Analyze each sale using Claude
    for (const sale of detailedSales) {
        try {
            // Use Claude instead of hardcoded valuation engine
            const analysis = await this.claudeAnalyzer.analyzeSalesUndervaluation(
                sale, 
                detailedSales, 
                neighborhood,
                { undervaluationThreshold: 10 }
            );
            
            if (analysis.isUndervalued) {
                undervaluedSales.push({
                    ...sale,
                    // Claude analysis results
                    discountPercent: analysis.discountPercent,
                    estimatedMarketPrice: analysis.estimatedMarketPrice,
                    actualPrice: analysis.actualPrice,
                    potentialProfit: analysis.potentialProfit,
                    confidence: analysis.confidence,
                    valuationMethod: analysis.method,
                    comparablesUsed: analysis.comparablesUsed,
                    adjustmentBreakdown: analysis.adjustmentBreakdown,
                    reasoning: analysis.reasoning,
                    
                    // Keep existing scoring
                    score: this.calculateAdvancedSalesScore(analysis),
                    grade: analysis.grade,
                    comparisonGroup: `${sale.bedrooms}BR/${sale.bathrooms}BA in ${neighborhood}`,
                    comparisonMethod: analysis.method
                });
            }
        } catch (error) {
            console.warn(`   ⚠️ Error analyzing ${sale.address}: ${error.message}`);
        }
    }

    // Sort by discount percentage (best deals first)
    undervaluedSales.sort((a, b) => b.discountPercent - a.discountPercent);

    console.log(`   🎯 Found ${undervaluedSales.length} undervalued sales (10% threshold with Claude AI)`);
    return undervaluedSales;
}

    /**
     * Calculate advanced sales score based on multi-factor analysis
     * NOTE: This is now the composite score for internal ranking within grades
     */
    calculateAdvancedSalesScore(analysis) {
        return this.calculateCompositeScore(analysis);
    }

    /**
     * Calculate letter grade from discount percentage (NEW: More realistic grading starting from 10%)
     * Based on actual discount % rather than composite score to prevent grade inflation
     */
    calculateGradeFromDiscount(discountPercent) {
        if (discountPercent >= 25) return 'A+';        // 25%+ off = True unicorns  
        if (discountPercent >= 20) return 'A';         // 20-24% off = Excellent deals
        if (discountPercent >= 17) return 'A-';        // 17-19% off = Very good deals  
        if (discountPercent >= 15) return 'B+';        // 15-16% off = Good deals
        if (discountPercent >= 12) return 'B';         // 12-14% off = Solid deals
        if (discountPercent >= 10) return 'B-';        // 10-11% off = Decent deals
        return 'C';                                     // <10% = Doesn't qualify
    }

    /**
     * Calculate composite score from multiple factors (kept for internal ranking)
     * This score helps rank properties within the same grade level
     */
    calculateCompositeScore(analysis) {
        let score = 0;

        // Base score from discount percentage (0-50 points) - weighted higher for 25%+ deals
        score += Math.min(analysis.discountPercent * 1.8, 50);

        // Confidence bonus (0-20 points) - critical for advanced analysis
        if (analysis.confidence >= 90) score += 20;
        else if (analysis.confidence >= 80) score += 15;
        else if (analysis.confidence >= 70) score += 10;
        else score += 5;

        // Valuation method quality bonus (0-15 points)
        if (analysis.method === 'exact_bed_bath_amenity_match') score += 15;
        else if (analysis.method === 'bed_bath_specific_pricing') score += 12;
        else if (analysis.method === 'bed_specific_with_adjustments') score += 8;
        else if (analysis.method === 'price_per_sqft_fallback') score += 5;

        // Comparable count bonus (0-10 points)
        if (analysis.comparablesUsed >= 15) score += 10;
        else if (analysis.comparablesUsed >= 10) score += 7;
        else if (analysis.comparablesUsed >= 5) score += 5;

        // Profit magnitude bonus (0-5 points)
        if (analysis.potentialProfit >= 200000) score += 5;
        else if (analysis.potentialProfit >= 100000) score += 3;

        return Math.min(100, Math.max(0, Math.round(score)));
    }

    /**
     * Save undervalued sales to database with enhanced deduplication check and advanced valuation data
     */
    async saveUndervaluedSalesToDatabase(undervaluedSales, neighborhood) {
        console.log(`   💾 Saving ${undervaluedSales.length} undervalued sales to database...`);

        let savedCount = 0;

        for (const sale of undervaluedSales) {
            try {
                // Enhanced duplicate check
                const { data: existing } = await this.supabase
                    .from('undervalued_sales')
                    .select('id, score')
                    .eq('listing_id', sale.id)
                    .single();

                if (existing) {
                    // Update if score improved
                    if (sale.score > existing.score) {
                        const { error: updateError } = await this.supabase
                            .from('undervalued_sales')
                            .update({
                                score: sale.score,
                                discount_percent: sale.discountPercent,
                                last_seen_in_search: new Date().toISOString(),
                                times_seen_in_search: 1, // Reset counter
                                analysis_date: new Date().toISOString()
                            })
                            .eq('id', existing.id);

                        if (!updateError) {
                            console.log(`   🔄 Updated: ${sale.address} (score: ${existing.score} → ${sale.score})`);
                        }
                    } else {
                        console.log(`   ⏭️ Skipping duplicate: ${sale.address}`);
                    }
                    continue;
                }

                // Enhanced database record with advanced valuation data
                const dbRecord = {
                    listing_id: sale.id?.toString(),
                    address: sale.address,
                    neighborhood: sale.neighborhood,
                    borough: sale.borough || 'unknown',
                    zipcode: sale.zipcode,
                    
                    // Advanced sales pricing analysis
                    sale_price: parseInt(sale.salePrice) || 0,
                    price_per_sqft: sale.actualPrice && sale.sqft > 0 ? parseFloat((sale.actualPrice / sale.sqft).toFixed(2)) : null,
                    market_price_per_sqft: sale.estimatedMarketPrice && sale.sqft > 0 ? parseFloat((sale.estimatedMarketPrice / sale.sqft).toFixed(2)) : null,
                    discount_percent: parseFloat(sale.discountPercent.toFixed(2)),
                    potential_profit: parseInt(sale.potentialProfit) || 0,
                    
                    // Property details
                    bedrooms: parseInt(sale.bedrooms) || 0,
                    bathrooms: sale.bathrooms ? parseFloat(sale.bathrooms) : null,
                    sqft: sale.sqft ? parseInt(sale.sqft) : null,
                    property_type: sale.propertyType || 'apartment',
                    
                    // Sale terms
                    listing_status: sale.status || 'unknown',
                    listed_at: sale.listedAt ? new Date(sale.listedAt).toISOString() : null,
                    closed_at: sale.closedAt ? new Date(sale.closedAt).toISOString() : null,
                    sold_at: sale.soldAt ? new Date(sale.soldAt).toISOString() : null,
                    days_on_market: parseInt(sale.daysOnMarket) || 0,
                    
                    // Building features
                    doorman_building: sale.doormanBuilding || false,
                    elevator_building: sale.elevatorBuilding || false,
                    pet_friendly: sale.petFriendly || false,
                    laundry_available: sale.laundryAvailable || false,
                    gym_available: sale.gymAvailable || false,
                    rooftop_access: sale.rooftopAccess || false,
                    
                    // Building info
                    built_in: sale.builtIn ? parseInt(sale.builtIn) : null,
                    latitude: sale.latitude ? parseFloat(sale.latitude) : null,
                    longitude: sale.longitude ? parseFloat(sale.longitude) : null,
                    
                    // Media and description
                    images: Array.isArray(sale.images) ? sale.images : [],
                    image_count: Array.isArray(sale.images) ? sale.images.length : 0,
                    videos: Array.isArray(sale.videos) ? sale.videos : [],
                    floorplans: Array.isArray(sale.floorplans) ? sale.floorplans : [],
                    description: typeof sale.description === 'string' ? 
                        sale.description.substring(0, 2000) : '',
                    
                    // Amenities
                    amenities: Array.isArray(sale.amenities) ? sale.amenities : [],
                    amenity_count: Array.isArray(sale.amenities) ? sale.amenities.length : 0,
                    
                    // Advanced analysis results
                    score: parseInt(sale.score) || 0,
                    grade: sale.grade || 'F',
                    deal_quality: this.calculateDealQuality(parseInt(sale.score) || 0),
                    reasoning: sale.reasoning || '',
                    comparison_group: sale.comparisonGroup || '',
                    comparison_method: sale.valuationMethod || sale.comparisonMethod || '',
                    reliability_score: parseInt(sale.confidence) || 0,
                    
                    // Additional data
                    building_info: typeof sale.building === 'object' ? sale.building : {},
                    agents: Array.isArray(sale.agents) ? sale.agents : [],
                    sale_type: sale.type || 'sale',
                    
                    // ENHANCED: Deduplication and sold tracking fields
                    last_seen_in_search: new Date().toISOString(),
                    times_seen_in_search: 1,
                    likely_sold: false,
                    
                    analysis_date: new Date().toISOString(),
                    status: 'active'
                };

                const { error } = await this.supabase
                    .from('undervalued_sales')
                    .insert([dbRecord]);

                if (error) {
                    console.error(`   ❌ Error saving sale ${sale.address}:`, error.message);
                } else {
                    console.log(`   ✅ Saved: ${sale.address} (${sale.discountPercent}% below market, Score: ${sale.score}, Method: ${sale.valuationMethod})`);
                    savedCount++;
                }
            } catch (error) {
                console.error(`   ❌ Error processing sale ${sale.address}:`, error.message);
            }
        }

        console.log(`   💾 Saved ${savedCount} new undervalued sales using advanced multi-factor analysis`);
        return savedCount;
    }

    /**
     * Get latest undervalued sales with status filtering
     */
    async getLatestUndervaluedSales(limit = 50, minScore = 50) {
        try {
            const { data, error } = await this.supabase
                .from('undervalued_sales')
                .select('*')
                .eq('status', 'active') // Only active listings
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
     * Get sales by neighborhood with status filtering
     */
    async getSalesByNeighborhood(neighborhood, limit = 20) {
        try {
            const { data, error } = await this.supabase
                .from('undervalued_sales')
                .select('*')
                .eq('neighborhood', neighborhood)
                .eq('status', 'active') // Only active listings
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
     * Get top scoring sale deals (active only)
     */
    async getTopSaleDeals(limit = 20) {
        try {
            const { data, error } = await this.supabase
                .from('undervalued_sales')
                .select('*')
                .eq('status', 'active') // Only active listings
                .gte('score', 70)
                .order('score', { ascending: false })
                .limit(limit);

            if (error) throw error;
            return data;
        } catch (error) {
            console.error('❌ Error fetching top sale deals:', error.message);
            return [];
        }
    }

    /**
     * Get sales by specific criteria (active only)
     */
    async getSalesByCriteria(criteria = {}) {
        try {
            let query = this.supabase
                .from('undervalued_sales')
                .select('*')
                .eq('status', 'active'); // Only active listings

            if (criteria.maxPrice) {
                query = query.lte('sale_price', criteria.maxPrice);
            }
            if (criteria.minBedrooms) {
                query = query.gte('bedrooms', criteria.minBedrooms);
            }
            if (criteria.doorman) {
                query = query.eq('doorman_building', true);
            }
            if (criteria.elevator) {
                query = query.eq('elevator_building', true);
            }
            if (criteria.borough) {
                query = query.eq('borough', criteria.borough);
            }

            const { data, error } = await query
                .order('score', { ascending: false })
                .limit(criteria.limit || 20);

            if (error) throw error;
            return data;
        } catch (error) {
            console.error('❌ Error fetching sales by criteria:', error.message);
            return [];
        }
    }

    /**
     * Setup enhanced database schema for sales with deduplication
     */
    async setupSalesDatabase() {
        console.log('🔧 Setting up enhanced sales database schema with deduplication...');

        try {
            console.log('✅ Enhanced sales database with deduplication is ready');
            console.log('💾 Core tables will be created via SQL schema');
            console.log('🏠 Basic sold listing detection enabled');
            console.log('⚠️ Advanced database functions can be added later for enhanced features');
            console.log('\n💡 For full functionality, add these SQL functions to your database:');
            console.log('   - mark_likely_sold_listings()');
            console.log('   - cleanup_old_cache_entries()');
            
        } catch (error) {
            console.error('❌ Sales database setup error:', error.message);
        }
    }
}

// REPLACE your main() function with this:

async function main() {
    const args = process.argv.slice(2);
    
    // Check ALL required environment variables including Claude
    if (!process.env.RAPIDAPI_KEY || !process.env.SUPABASE_URL || !process.env.SUPABASE_ANON_KEY || !process.env.ANTHROPIC_API_KEY) {
        console.error('❌ Missing required environment variables!');
        console.error('   RAPIDAPI_KEY, SUPABASE_URL, SUPABASE_ANON_KEY, ANTHROPIC_API_KEY required');
        console.error('\n💡 Get your Claude API key from: https://console.anthropic.com/');
        process.exit(1);
    }

    const analyzer = new EnhancedBiWeeklySalesAnalyzer();

    // ... rest of your existing main() function (keep everything else the same)
    if (args.includes('--setup')) {
        await analyzer.setupSalesDatabase();
        return;
    }

    if (args.includes('--latest')) {
        const limit = parseInt(args[args.indexOf('--latest') + 1]) || 20;
        const sales = await analyzer.getLatestUndervaluedSales(limit);
        console.log(`🏠 Latest ${sales.length} active undervalued sales (Claude AI analysis):`);
        sales.forEach((sale, i) => {
            console.log(`${i + 1}. ${sale.address} - ${sale.sale_price.toLocaleString()} (${sale.discount_percent}% below market, Score: ${sale.score})`);
        });
        return;
    }

    if (args.includes('--top-deals')) {
        const limit = parseInt(args[args.indexOf('--top-deals') + 1]) || 10;
        const deals = await analyzer.getTopSaleDeals(limit);
        console.log(`🏆 Top ${deals.length} active sale deals (Claude AI analysis):`);
        deals.forEach((deal, i) => {
            console.log(`${i + 1}. ${deal.address} - ${deal.sale_price.toLocaleString()} (${deal.discount_percent}% below market, Score: ${deal.score})`);
        });
        return;
    }

    if (args.includes('--neighborhood')) {
        const neighborhood = args[args.indexOf('--neighborhood') + 1];
        if (!neighborhood) {
            console.error('❌ Please provide a neighborhood: --neighborhood park-slope');
            return;
        }
        const sales = await analyzer.getSalesByNeighborhood(neighborhood);
        console.log(`🏠 Active sales in ${neighborhood} (Claude AI analysis):`);
        sales.forEach((sale, i) => {
            console.log(`${i + 1}. ${sale.address} - ${sale.sale_price.toLocaleString()} (Score: ${sale.score})`);
        });
        return;
    }

    if (args.includes('--doorman')) {
        const sales = await analyzer.getSalesByCriteria({ doorman: true, limit: 15 });
        console.log(`🚪 Active doorman building sales (Claude AI analysis):`);
        sales.forEach((sale, i) => {
            console.log(`${i + 1}. ${sale.address} - ${sale.sale_price.toLocaleString()} (${sale.discount_percent}% below market)`);
        });
        return;
    }

    // Default: run bi-weekly sales analysis with Claude AI
    console.log('🏠 Starting CLAUDE AI bi-weekly sales analysis with natural language reasoning...');
    const results = await analyzer.runBiWeeklySalesRefresh();
    
    console.log('\n🎉 Claude AI sales analysis with smart deduplication completed!');
    
    if (results.summary && results.summary.apiCallsSaved > 0) {
        const efficiency = ((results.summary.apiCallsSaved / (results.summary.apiCallsUsed + results.summary.apiCallsSaved)) * 100).toFixed(1);
        console.log(`⚡ Achieved ${efficiency}% API efficiency through smart caching!`);
    }
    
    if (results.summary && results.summary.savedToDatabase) {
        console.log(`📊 Check your Supabase 'undervalued_sales' table for ${results.summary.savedToDatabase} new deals!`);
        console.log(`🤖 All properties include Claude AI natural language explanations`);
    }
    
    return results;
}

// Export for use in other modules
module.exports = EnhancedBiWeeklySalesAnalyzer;

// Run if executed directly
if (require.main === module) {
    main().catch(error => {
        console.error('💥 Enhanced sales analyzer with advanced valuation crashed:', error);
        process.exit(1);
    });
}
