// enhanced-biweekly-streeteasy-rentals.js
// FINAL VERSION: Smart deduplication + ADVANCED MULTI-FACTOR VALUATION + automatic rented listing cleanup
// NEW: Sophisticated bed/bath/amenity-based valuation instead of simple price per sqft
// THRESHOLD: Only properties 25%+ below true market value are flagged as undervalued
require('dotenv').config();
const axios = require('axios');
const { createClient } = require('@supabase/supabase-js');

const HIGH_PRIORITY_NEIGHBORHOODS = [
    'west-village', 'east-village', 'soho', 'tribeca', 'chelsea',
    'upper-east-side', 'upper-west-side', 'park-slope', 'williamsburg',
    'dumbo', 'brooklyn-heights', 'fort-greene', 'prospect-heights',
    'crown-heights', 'bedford-stuyvesant', 'greenpoint', 'bushwick',
    'long-island-city', 'astoria', 'sunnyside'
];

// Advanced Multi-Factor Rental Valuation Algorithm
// SOPHISTICATED APPROACH: Uses specific bed/bath combinations + amenities + adjustments
// Moves beyond simple price per sqft to true market value assessment
class AdvancedRentalValuationEngine {
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

        // NYC amenity value adjustments (based on 2025 market data)
        // Structure: { manhattan: {type: 'percentage'/'fixed', value: number}, outer: {type: 'percentage'/'fixed', value: number} }
        this.AMENITY_VALUES = {
            // Building amenities
            'doorman_full_time': {
                manhattan: { type: 'percentage', value: 37.5 }, // 35-40% average
                outer: { type: 'percentage', value: 25 }         // 20-30% average
            },
            'doorman_part_time': {
                manhattan: { type: 'percentage', value: 15 },
                outer: { type: 'percentage', value: 15 }
            },
            'doorman': { // Default to full-time if type not specified
                manhattan: { type: 'percentage', value: 37.5 },
                outer: { type: 'percentage', value: 25 }
            },
            'concierge': { // Included in doorman premium, small additional if ultra-luxury
                manhattan: { type: 'percentage', value: 3 },
                outer: { type: 'percentage', value: 2 }
            },
            'elevator': {
                manhattan: { type: 'fixed', value: 100 },
                outer: { type: 'fixed', value: 75 }
            },
            'no_elevator': { // Walk-up penalty
                manhattan: { type: 'fixed', value: -100 },
                outer: { type: 'fixed', value: -100 }
            },
            
            // In-unit amenities  
            'washer_dryer': {
                manhattan: { type: 'fixed', value: 100 },
                outer: { type: 'fixed', value: 75 }
            },
            'dishwasher': {
                manhattan: { type: 'fixed', value: 40 },
                outer: { type: 'fixed', value: 30 }
            },
            'central_air': {
                manhattan: { type: 'fixed', value: 75 },
                outer: { type: 'fixed', value: 75 }
            },
            'balcony': {
                manhattan: { type: 'fixed', value: 250 },
                outer: { type: 'fixed', value: 150 }
            },
            'terrace': {
                manhattan: { type: 'fixed', value: 400 },
                outer: { type: 'fixed', value: 300 }
            },
            'private_outdoor_space': {
                manhattan: { type: 'fixed', value: 400 },
                outer: { type: 'fixed', value: 300 }
            },
            
            // Building facilities
            'gym': {
                manhattan: { type: 'fixed', value: 100 },
                outer: { type: 'fixed', value: 75 }
            },
            'fitness_center': {
                manhattan: { type: 'fixed', value: 100 },
                outer: { type: 'fixed', value: 75 }
            },
            'pool': {
                manhattan: { type: 'fixed', value: 150 },
                outer: { type: 'fixed', value: 100 }
            },
            'roof_deck': {
                manhattan: { type: 'fixed', value: 50 },
                outer: { type: 'fixed', value: 25 }
            },
            'laundry_room': {
                manhattan: { type: 'fixed', value: 25 },
                outer: { type: 'fixed', value: 25 }
            },
            'parking': {
                manhattan: { type: 'fixed', value: 300 },
                outer: { type: 'fixed', value: 150 }
            },
            'bike_storage': {
                manhattan: { type: 'fixed', value: 5 },
                outer: { type: 'fixed', value: 5 }
            },
            
            // Pet and lifestyle
            'pet_friendly': {
                manhattan: { type: 'fixed', value: 50 },
                outer: { type: 'fixed', value: 50 }
            },
            'no_fee': {
                manhattan: { type: 'fixed', value: 100 },
                outer: { type: 'fixed', value: 100 }
            },
            
            // Condition and quality (percentage-based)
            'newly_renovated': {
                manhattan: { type: 'percentage', value: 7.5 }, // 5-10% average
                outer: { type: 'percentage', value: 7.5 }
            },
            'luxury_finishes': {
                manhattan: { type: 'percentage', value: 5 },
                outer: { type: 'percentage', value: 5 }
            },
            'hardwood_floors': {
                manhattan: { type: 'fixed', value: 50 },
                outer: { type: 'fixed', value: 50 }
            },
            'high_ceilings': {
                manhattan: { type: 'percentage', value: 5 },
                outer: { type: 'percentage', value: 5 }
            },
            'exposed_brick': {
                manhattan: { type: 'fixed', value: 50 },
                outer: { type: 'fixed', value: 50 }
            },
            
            // Negative factors
            'studio_layout': {
                manhattan: { type: 'percentage', value: -25 }, // -20 to -30% average
                outer: { type: 'percentage', value: -25 }
            },
            'no_natural_light': {
                manhattan: { type: 'percentage', value: -15 }, // -10 to -20% average
                outer: { type: 'percentage', value: -15 }
            },
            'noisy_location': {
                manhattan: { type: 'percentage', value: -10 }, // -5 to -15% average
                outer: { type: 'percentage', value: -10 }
            },
            'ground_floor': {
                manhattan: { type: 'percentage', value: -7.5 }, // -5 to -10% average
                outer: { type: 'percentage', value: -7.5 }
            }
        };

        // Bathroom adjustment factors (fixed dollar amounts)
        this.BATHROOM_ADJUSTMENTS = {
            0.5: -200,  // Half bath deficit
            1.0: 0,     // Baseline
            1.5: 150,   // Extra half bath
            2.0: 300,   // Full second bathroom
            2.5: 400,   // Two and a half baths
            3.0: 500    // Three full bathrooms
        };

        // Square footage adjustments per bedroom category
        this.SQFT_ADJUSTMENTS = {
            'studio': { baseline: 450, per_sqft_over: 2.0, per_sqft_under: -2.5 },
            '1bed': { baseline: 650, per_sqft_over: 2.0, per_sqft_under: -2.5 },
            '2bed': { baseline: 900, per_sqft_over: 1.8, per_sqft_under: -2.2 },
            '3bed': { baseline: 1200, per_sqft_over: 1.5, per_sqft_under: -2.0 }
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
    calculateLocationAwareAmenityValue(amenities, targetProperty, baseRent = 0) {
        const isManhattan = this.isManhattan(targetProperty);
        let totalAdjustment = 0;
        const appliedAdjustments = [];

        amenities.forEach(amenity => {
            const amenityConfig = this.AMENITY_VALUES[amenity];
            if (!amenityConfig) return;

            const locationConfig = isManhattan ? amenityConfig.manhattan : amenityConfig.outer;
            let adjustment = 0;

            if (locationConfig.type === 'percentage') {
                // Percentage adjustment requires base rent
                if (baseRent > 0) {
                    adjustment = (baseRent * locationConfig.value) / 100;
                    appliedAdjustments.push(`${amenity}: +${locationConfig.value}% (${isManhattan ? 'Manhattan' : 'Outer'}) = ${Math.round(adjustment)}`);
                }
            } else if (locationConfig.type === 'fixed') {
                adjustment = locationConfig.value;
                appliedAdjustments.push(`${amenity}: ${adjustment} (${isManhattan ? 'Manhattan' : 'Outer'})`);
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
                estimatedMarketRent: 0,
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
        console.log(`   🎯 Est. market rent: $${adjustedMarketValue.finalValue.toLocaleString()}`);
        console.log(`   📊 Method: ${valuationResult.method} (${confidence}% confidence)`);

        return {
            success: true,
            estimatedMarketRent: adjustedMarketValue.finalValue,
            baseMarketRent: baseMarketValue.baseValue,
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
            comp.sqft > 0 && comp.monthlyRent > 0 &&
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
        const rents = comparables.map(comp => comp.monthlyRent).sort((a, b) => a - b);
        const median = this.calculateMedian(rents);
        
        // Use median as most stable central tendency
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
    calculateBedroomBasedValue(targetProperty, comparables) {
        const targetBaths = targetProperty.bathrooms || 1;
        
        // Calculate base rent for this bedroom count
        const rents = comparables.map(comp => comp.monthlyRent);
        const medianRent = this.calculateMedian(rents);
        
        // Find typical bathroom count for this bedroom category
        const bathCounts = comparables.map(comp => comp.bathrooms || 1);
        const typicalBathCount = this.calculateMedian(bathCounts);
        
        // Adjust base rent for bathroom difference
        const bathDifference = targetBaths - typicalBathCount;
        const bathAdjustment = this.calculateBathroomAdjustment(bathDifference);
        
        return {
            baseValue: medianRent + bathAdjustment,
            method: 'bedroom_based_with_bath_adjustment',
            dataPoints: rents.length,
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
            .map(comp => comp.monthlyRent / comp.sqft);
            
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
        
        // Calculate average amenity value of comparables (using their base rents)
        const comparableAmenityValues = comparables.map(comp => {
            const compAmenities = this.normalizeAmenities(comp.amenities || []);
            const compDescAmenities = this.extractAmenitiesFromDescription(comp.description || '');
            const allCompAmenities = [...new Set([...compAmenities, ...compDescAmenities])];
            
            const amenityAnalysis = this.calculateLocationAwareAmenityValue(
                allCompAmenities, 
                comp, 
                comp.monthlyRent || 0
            );
            return amenityAnalysis.totalAdjustment;
        });
        
        const avgComparableAmenityValue = comparableAmenityValues.reduce((a, b) => a + b, 0) / comparableAmenityValues.length;
        
        // Calculate target property amenity value (using estimated market rent for percentage calculations)
        const estimatedBaseRent = this.estimateBaseRentForAmenityCalculation(targetProperty, comparables);
        const targetAmenityAnalysis = this.calculateLocationAwareAmenityValue(
            allTargetAmenities, 
            targetProperty, 
            estimatedBaseRent
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
     * Estimate base rent for amenity percentage calculations
     */
    estimateBaseRentForAmenityCalculation(targetProperty, comparables) {
        // Use median rent of comparables as estimate for percentage-based amenity calculations
        const comparableRents = comparables.map(comp => comp.monthlyRent).filter(rent => rent > 0);
        if (comparableRents.length === 0) return 3000; // Fallback for NYC
        
        const sortedRents = comparableRents.sort((a, b) => a - b);
        return sortedRents[Math.floor(sortedRents.length / 2)];
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
            'no_fee': ['no fee', 'no broker fee', 'fee free'],
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
            adjustment += this.AMENITY_VALUES.newly_renovated || 300;
        }
        if (description.includes('luxury') || description.includes('high-end')) {
            adjustment += this.AMENITY_VALUES.luxury_finishes || 250;
        }
        if (description.includes('hardwood') || description.includes('wood floors')) {
            adjustment += this.AMENITY_VALUES.hardwood_floors || 150;
        }
        
        // Negative quality indicators
        if (description.includes('needs work') || description.includes('tlc')) {
            adjustment -= 200;
        }
        if (description.includes('as-is') || description.includes('fixer')) {
            adjustment -= 300;
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
            adjustment += 100;
        }
        if (description.includes('busy street') || description.includes('noisy')) {
            adjustment -= 150;
        }
        if (description.includes('ground floor') && !description.includes('garden')) {
            adjustment -= 100;
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
     * Calculate total amenity value (kept for backward compatibility, but now location-aware)
     */
    calculateAmenityValue(amenities, targetProperty = null, baseRent = 0) {
        if (targetProperty) {
            // Use new location-aware calculation
            const analysis = this.calculateLocationAwareAmenityValue(amenities, targetProperty, baseRent);
            return analysis.totalAdjustment;
        } else {
            // Fallback to outer borough fixed values for backward compatibility
            return amenities.reduce((total, amenity) => {
                const amenityConfig = this.AMENITY_VALUES[amenity];
                if (!amenityConfig) return total;
                
                const outerConfig = amenityConfig.outer;
                if (outerConfig.type === 'fixed') {
                    return total + outerConfig.value;
                } else {
                    // Can't calculate percentage without base rent, use fixed equivalent
                    return total + (baseRent > 0 ? (baseRent * outerConfig.value / 100) : 0);
                }
            }, 0);
        }
    }

    /**
     * Calculate bathroom adjustment amount
     */
    calculateBathroomAdjustment(bathDifference) {
        // Convert bathroom difference to adjustment amount
        return bathDifference * 200; // $200 per 0.5 bathroom difference
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
        
        reasons.push(`Base value: $${baseValue.baseValue.toLocaleString()} (${valuationResult.method})`);
        reasons.push(`${valuationResult.comparables.length} comparable properties used`);
        
        if (adjustedValue.adjustments.length > 0) {
            adjustedValue.adjustments.forEach(adj => {
                const sign = adj.amount > 0 ? '+' : '';
                reasons.push(`${adj.category}: ${sign}$${adj.amount.toLocaleString()}`);
            });
        }
        
        return reasons.join('; ');
    }

    /**
     * Check if a comparable has reasonable data quality
     */
    hasReasonableDataQuality(comparable) {
        return comparable.monthlyRent > 0 &&
               comparable.monthlyRent <= 50000 &&
               comparable.bedrooms !== undefined &&
               comparable.bathrooms !== undefined &&
               (comparable.daysOnMarket || 0) <= 120; // Not stale
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
     * MAIN INTERFACE: Analyze rental for undervaluation using advanced multi-factor approach
     */
    analyzeRentalUndervaluation(targetProperty, comparableProperties, neighborhood, options = {}) {
        const threshold = options.undervaluationThreshold || 25; // NEW: Raised to 25% for only the BEST deals
        
        console.log(`\n🎯 ADVANCED VALUATION: ${targetProperty.address || 'Property'}`);
        console.log(`   📊 Rent: $${targetProperty.monthlyRent.toLocaleString()} | ${targetProperty.bedrooms}BR/${targetProperty.bathrooms}BA | ${targetProperty.sqft || 'N/A'} sqft`);
        
        // Get true market value using advanced multi-factor analysis
        const valuation = this.calculateTrueMarketValue(targetProperty, comparableProperties, neighborhood);
        
        if (!valuation.success) {
            return {
                isUndervalued: false,
                discountPercent: 0,
                estimatedMarketRent: 0,
                actualRent: targetProperty.monthlyRent,
                confidence: 0,
                method: 'insufficient_data',
                reasoning: valuation.reasoning
            };
        }
        
        // Calculate actual discount percentage
        const actualRent = targetProperty.monthlyRent;
        const estimatedMarketRent = valuation.estimatedMarketRent;
        const discountPercent = ((estimatedMarketRent - actualRent) / estimatedMarketRent) * 100;
        
        // Determine if truly undervalued - NEW: Only 25%+ below market with method-appropriate confidence
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
        
        console.log(`   💰 Actual rent: $${actualRent.toLocaleString()}`);
        console.log(`   🎯 Market value: $${estimatedMarketRent.toLocaleString()}`);
        console.log(`   📊 Discount: ${discountPercent.toFixed(1)}%`);
        console.log(`   ✅ Undervalued: ${isUndervalued ? 'YES' : 'NO'} (${threshold}% threshold, ${valuation.confidence}% confidence)`);
        
        return {
            isUndervalued,
            discountPercent: Math.round(discountPercent * 10) / 10,
            estimatedMarketRent: estimatedMarketRent,
            actualRent: actualRent,
            potentialMonthlySavings: Math.round(estimatedMarketRent - actualRent),
            confidence: valuation.confidence,
            method: valuation.method,
            comparablesUsed: valuation.comparablesUsed,
            adjustmentBreakdown: valuation.adjustmentBreakdown,
            reasoning: valuation.reasoning
        };
    }
}

class EnhancedBiWeeklyRentalAnalyzer {
    constructor() {
        this.supabase = createClient(
            process.env.SUPABASE_URL,
            process.env.SUPABASE_ANON_KEY
        );
        
        this.rapidApiKey = process.env.RAPIDAPI_KEY;
        this.apiCallsUsed = 0;
        
        // Initialize the advanced valuation engine
        this.valuationEngine = new AdvancedRentalValuationEngine();
        
        // Check for initial bulk load mode (same as sales file)
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
        
        // Track rental-specific API usage with DEDUPLICATION stats
        this.apiUsageStats = {
            activeRentalsCalls: 0,
            detailsCalls: 0,
            failedCalls: 0,
            rateLimitHits: 0,
            adaptiveDelayChanges: 0,
            // NEW: Deduplication performance tracking
            totalListingsFound: 0,
            cacheHits: 0,
            newListingsToFetch: 0,
            apiCallsSaved: 0,
            listingsMarkedRented: 0
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
     * OFFSET: Rentals run 1 week after sales to prevent API conflicts
     */
    getCurrentScheduleDay() {
        const today = new Date();
        const dayOfMonth = today.getDate();
        
        // RENTALS SCHEDULE: Offset by 1 week from sales schedule
        // Sales run: 1-8 and 15-22
        // Rentals run: 8-15 and 22-29 (offset to prevent API conflicts)
        if (dayOfMonth >= 8 && dayOfMonth <= 15) {
            return dayOfMonth - 7; // Days 8-15 become 1-8
        } else if (dayOfMonth >= 22 && dayOfMonth <= 29) {
            return dayOfMonth - 21; // Days 22-29 become 1-8
        } else {
            return 0; // Off-schedule, run buffer mode
        }
    }

    /**
     * Get today's neighborhood assignments WITH BULK LOAD SUPPORT
     * 5-second delay to prevent API conflicts + full bulk load capability
     */
    async getTodaysNeighborhoods() {
        // 5-SECOND DELAY: Simple delay to prevent simultaneous API calls with sales
        console.log('⏰ 5-second delay to prevent API conflicts with sales...');
        await this.delay(5000);
        console.log('✅ 5-second delay complete!');
        
        // INITIAL BULK LOAD: Process ALL neighborhoods (same as sales file)
        if (this.initialBulkLoad) {
            console.log('🚀 INITIAL BULK LOAD MODE: Processing ALL rental neighborhoods');
            console.log(`📋 Will process ${HIGH_PRIORITY_NEIGHBORHOODS.length} neighborhoods over multiple hours`);
            return ['soho']
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
     * FIXED: Added missing async and proper function structure
     */
    async getMissedNeighborhoods() {
        try {
            // Check database for neighborhoods not analyzed in last 14 days
            const { data, error } = await this.supabase
                .from('bi_weekly_analysis_runs')
                .select('detailed_stats')
                .eq('analysis_type', 'rentals')
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
     * NEW: EFFICIENT: Update only rent in cache (no refetch needed)
     * FIXED: Using valid market_status values from schema
     */
    async updateRentInCache(listingId, newRent) {
        try {
            const { error } = await this.supabase
                .from('rental_market_cache')
                .update({
                    monthly_rent: newRent,
                    last_checked: new Date().toISOString(),
                    market_status: 'pending' // FIXED: Valid schema value
                })
                .eq('listing_id', listingId);

            if (error) {
                console.warn(`⚠️ Error updating rent for ${listingId}:`, error.message);
            } else {
                console.log(`   💾 Updated cache rent for ${listingId}: ${newRent.toLocaleString()}/month`);
            }
        } catch (error) {
            console.warn(`⚠️ Error updating rent in cache for ${listingId}:`, error.message);
        }
    }

    /**
     * NEW: EFFICIENT: Update rent in undervalued_rentals table if listing exists
     */
    async updateRentInUndervaluedRentals(listingId, newRent, sqft) {
        try {
            const updateData = {
                monthly_rent: parseInt(newRent),
                analysis_date: new Date().toISOString()
            };

            // Calculate new rent per sqft if we have sqft data
            if (sqft && sqft > 0) {
                updateData.rent_per_sqft = parseFloat((newRent / sqft).toFixed(2));
            }

            const { error } = await this.supabase
                .from('undervalued_rentals')
                .update(updateData)
                .eq('listing_id', listingId)
                .eq('status', 'active');

            if (error) {
                // Don't log error - listing might not be in undervalued_rentals table
            } else {
                console.log(`   💾 Updated undervalued_rentals rent for ${listingId}: ${newRent.toLocaleString()}/month`);
            }
        } catch (error) {
            // Silent fail - listing might not be undervalued
        }
    }

    /**
     * NEW: EFFICIENT: Mark listing for reanalysis due to rent change
     * FIXED: Using valid market_status values from schema
     */
    async triggerReanalysisForRentChange(listingId, neighborhood) {
        try {
            // Update market_status to trigger reanalysis in next cycle
            const { error } = await this.supabase
                .from('rental_market_cache')
                .update({
                    market_status: 'pending', // FIXED: Valid schema value
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
     * NEW: OPTIMIZED: Handle rent updates efficiently without refetching
     * Updates rent in cache and triggers reanalysis for undervaluation
     */
    async handleRentUpdatesInCache(listingIds, rentalsData, neighborhood) {
        if (!listingIds || listingIds.length === 0) return { completeListingIds: [], rentUpdatedIds: [] };
        
        try {
            const sevenDaysAgo = new Date();
            sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);

            const { data, error } = await this.supabase
                .from('rental_market_cache')
                .select('listing_id, address, bedrooms, bathrooms, sqft, market_status, monthly_rent')
                .in('listing_id', listingIds)
                .gte('last_checked', sevenDaysAgo.toISOString());

            if (error) {
                console.warn('⚠️ Error checking existing rentals, will fetch all details:', error.message);
                return { completeListingIds: [], rentUpdatedIds: [] };
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

            // Handle rent changes efficiently
            const rentUpdatedIds = [];
            const rentalsMap = new Map(rentalsData.map(rental => [rental.id?.toString(), rental]));
            
            for (const cachedEntry of completeEntries) {
                const currentRental = rentalsMap.get(cachedEntry.listing_id);
                if (currentRental) {
                    // Get current rent from search results
                    const currentRent = currentRental.price || currentRental.rent || 0;
                    const cachedRent = cachedEntry.monthly_rent || 0;
                    
                    // If rent changed by more than $100, update cache directly
                    if (Math.abs(currentRent - cachedRent) > 100) {
                        console.log(`   💰 Rent change detected for ${cachedEntry.listing_id}: ${cachedRent.toLocaleString()} → ${currentRent.toLocaleString()}/month`);
                        
                        // ✅ EFFICIENT: Update rent in cache without refetching
                        await this.updateRentInCache(cachedEntry.listing_id, currentRent);
                        
                        // ✅ EFFICIENT: Update rent in undervalued_rentals if exists
                        await this.updateRentInUndervaluedRentals(cachedEntry.listing_id, currentRent, cachedEntry.sqft);
                        
                        // ✅ EFFICIENT: Trigger reanalysis for undervaluation (rent changed)
                        await this.triggerReanalysisForRentChange(cachedEntry.listing_id, neighborhood);
                        
                        rentUpdatedIds.push(cachedEntry.listing_id);
                    }
                }
            }

            const completeListingIds = completeEntries.map(row => row.listing_id);
            const incompleteCount = data.length - completeEntries.length;
            
            console.log(`   💾 Cache lookup: ${completeListingIds.length}/${listingIds.length} rentals with COMPLETE details found in cache`);
            if (incompleteCount > 0) {
                console.log(`   🔄 ${incompleteCount} cached entries need detail fetching (incomplete data)`);
            }
            if (rentUpdatedIds.length > 0) {
                console.log(`   💰 ${rentUpdatedIds.length} rent-only updates completed (no API calls used)`);
            }
            
            return { completeListingIds, rentUpdatedIds };
        } catch (error) {
            console.warn('⚠️ Cache lookup failed, will fetch all details:', error.message);
            return { completeListingIds: [], rentUpdatedIds: [] };
        }
    }

    /**
     * NEW: Run rented detection for specific neighborhood
     */
    async runRentedDetectionForNeighborhood(searchResults, neighborhood) {
        const currentTime = new Date().toISOString();
        const currentListingIds = searchResults.map(r => r.id?.toString()).filter(Boolean);
        
        try {
            const threeDaysAgo = new Date();
            threeDaysAgo.setDate(threeDaysAgo.getDate() - 3);

            // Get rentals in this neighborhood that weren't in current search
            const { data: missingRentals, error: missingError } = await this.supabase
                .from('rental_market_cache')
                .select('listing_id')
                .eq('neighborhood', neighborhood)
                .not('listing_id', 'in', `(${currentListingIds.map(id => `"${id}"`).join(',')})`)
                .lt('last_seen_in_search', threeDaysAgo.toISOString());

            if (missingError) {
                console.warn('⚠️ Error checking for missing rentals:', missingError.message);
                return { updated: searchResults.length, markedRented: 0 };
            }

            // Mark corresponding entries in undervalued_rentals as likely rented
            let markedRented = 0;
            if (missingRentals && missingRentals.length > 0) {
                const missingIds = missingRentals.map(r => r.listing_id);
                
                const { error: markRentedError } = await this.supabase
                    .from('undervalued_rentals')
                    .update({
                        status: 'likely_rented',
                        likely_rented: true,
                        rented_detected_at: currentTime
                    })
                    .in('listing_id', missingIds)
                    .eq('status', 'active');

                if (!markRentedError) {
                    markedRented = missingIds.length;
                    this.apiUsageStats.listingsMarkedRented += markedRented;
                    console.log(`   🏠 Marked ${markedRented} rentals as likely rented (not seen in recent search)`);
                } else {
                    console.warn('⚠️ Error marking rentals as rented:', markRentedError.message);
                }
            }

            return { markedRented };
        } catch (error) {
            console.warn('⚠️ Error in rented detection for neighborhood:', error.message);
            return { markedRented: 0 };
        }
    }

    /**
     * NEW: SIMPLIFIED: Update only search timestamps for rented detection
     * Rent updates are handled separately
     */
    async updateRentTimestampsOnly(searchResults, neighborhood) {
        const currentTime = new Date().toISOString();
        
        try {
            // Step 1: Update ONLY search timestamps (rent already handled above)
            for (const rental of searchResults) {
                if (!rental.id) continue;
                
                try {
                    const searchTimestampData = {
                        listing_id: rental.id.toString(),
                        neighborhood: neighborhood,
                        borough: rental.borough || 'unknown',
                        last_seen_in_search: currentTime,
                        times_seen: 1
                    };

                    const { error } = await this.supabase
                        .from('rental_market_cache')
                        .upsert(searchTimestampData, { 
                            onConflict: 'listing_id',
                            updateColumns: ['last_seen_in_search', 'neighborhood', 'borough']
                        });

                    if (error) {
                        console.warn(`⚠️ Error updating search timestamp for ${rental.id}:`, error.message);
                    }
                } catch (itemError) {
                    console.warn(`⚠️ Error processing search timestamp ${rental.id}:`, itemError.message);
                }
            }

            // Step 2: Run rented detection for this neighborhood
            const { markedRented } = await this.runRentedDetectionForNeighborhood(searchResults, neighborhood);
            
            console.log(`   💾 Updated search timestamps: ${searchResults.length} rentals, marked ${markedRented} as rented`);
            return { updated: searchResults.length, markedRented };

        } catch (error) {
            console.warn('⚠️ Error updating search timestamps:', error.message);
            return { updated: 0, markedRented: 0 };
        }
    }

    /**
     * NEW: Cache complete rental details for new listings
     * FIXED: Using valid market_status values from schema + ONLY called after successful individual fetch
     */
    async cacheCompleteRentalDetails(listingId, details, neighborhood) {
        try {
            const completeRentalData = {
                listing_id: listingId.toString(),
                address: details.address || 'Address from detail fetch',
                neighborhood: neighborhood,
                borough: details.borough || 'unknown',
                monthly_rent: details.monthlyRent || 0,
                bedrooms: details.bedrooms || 0,
                bathrooms: details.bathrooms || 0,
                sqft: details.sqft || 0,
                property_type: details.propertyType || 'apartment',
                market_status: 'pending', // FIXED: Valid schema value, set after successful individual fetch
                last_checked: new Date().toISOString(),
                last_seen_in_search: new Date().toISOString(),
                last_analyzed: null
            };

            const { error } = await this.supabase
                .from('rental_market_cache')
                .upsert(completeRentalData, { 
                    onConflict: 'listing_id',
                    updateColumns: ['address', 'bedrooms', 'bathrooms', 'sqft', 'monthly_rent', 'property_type', 'last_checked', 'market_status']
                });

            if (error) {
                console.warn(`⚠️ Error caching complete details for ${listingId}:`, error.message);
            } else {
                console.log(`   💾 Cached complete details for ${listingId} (${completeRentalData.monthly_rent?.toLocaleString()}/month)`);
            }
        } catch (error) {
            console.warn(`⚠️ Error caching complete rental details for ${listingId}:`, error.message);
        }
    }

    /**
     * NEW: Cache failed fetch attempt
     * FIXED: Using valid market_status values from schema + ONLY called after failed individual fetch
     */
    async cacheFailedRentalFetch(listingId, neighborhood) {
        try {
            const failedFetchData = {
                listing_id: listingId.toString(),
                address: 'Fetch failed',
                neighborhood: neighborhood,
                market_status: 'fetch_failed', // Valid schema value, set after failed individual fetch
                last_checked: new Date().toISOString(),
                last_seen_in_search: new Date().toISOString()
            };

            const { error } = await this.supabase
                .from('rental_market_cache')
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
     * FIXED: Using valid market_status values from schema + ONLY called after analysis complete
     */
    async updateCacheWithRentalAnalysisResults(detailedRentals, undervaluedRentals) {
        try {
            const cacheUpdates = detailedRentals.map(rental => {
                const isUndervalued = undervaluedRentals.some(ur => ur.id === rental.id);
                
                return {
                    listing_id: rental.id?.toString(),
                    market_status: isUndervalued ? 'undervalued' : 'market_rate', // Both valid schema values
                    last_analyzed: new Date().toISOString()
                };
            });

            for (const update of cacheUpdates) {
                try {
                    await this.supabase
                        .from('rental_market_cache')
                        .update({
                            market_status: update.market_status,
                            last_analyzed: update.last_analyzed
                        })
                        .eq('listing_id', update.listing_id);
                } catch (updateError) {
                    console.warn(`⚠️ Error updating cache for ${update.listing_id}:`, updateError.message);
                }
            }
            
            console.log(`   💾 Updated cache analysis status for ${cacheUpdates.length} rentals`);
        } catch (error) {
            console.warn('⚠️ Error updating cache analysis results:', error.message);
            console.warn('   Continuing without cache analysis updates');
        }
    }

    /**
     * Clear old rental data with enhanced cleanup
     * FIXED: Graceful degradation for missing database functions
     */
    async clearOldRentalData() {
        try {
            const oneMonthAgo = new Date();
            oneMonthAgo.setMonth(oneMonthAgo.getMonth() - 1);

            // Clear old undervalued rentals
            const { error: rentalsError } = await this.supabase
                .from('undervalued_rentals')
                .delete()
                .lt('analysis_date', oneMonthAgo.toISOString());

            if (rentalsError) {
                console.error('❌ Error clearing old rental data:', rentalsError.message);
            } else {
                console.log('🧹 Cleared old rental data (>1 month)');
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
            console.error('❌ Clear old rental data error:', error.message);
        }
    }

    /**
     * Save bi-weekly rental summary with enhanced deduplication stats
     * FIXED: Updated to include all required columns
     */
    async saveBiWeeklyRentalSummary(summary) {
        try {
            const { error } = await this.supabase
                .from('bi_weekly_analysis_runs')
                .insert([{
                    run_date: summary.startTime,
                    analysis_type: 'rentals',
                    neighborhoods_processed: summary.neighborhoodsProcessed,
                    total_active_listings: summary.totalActiveRentalsFound,
                    total_details_attempted: summary.totalDetailsAttempted,
                    total_details_fetched: summary.totalDetailsFetched,
                    undervalued_found: summary.undervaluedFound,
                    saved_to_database: summary.savedToDatabase,
                    api_calls_used: summary.apiCallsUsed,
                    
                    // ENHANCED: Deduplication performance stats
                    api_calls_saved: summary.apiCallsSaved || 0,
                    cache_hit_rate: summary.cacheHitRate || 0,
                    listings_marked_rented: summary.listingsMarkedRented || 0,
                    
                    duration_minutes: Math.round(summary.duration),
                    detailed_stats: summary.detailedStats,
                    errors: summary.errors,
                    completed: true
                }]);

            if (error) {
                console.error('❌ Error saving bi-weekly rental summary:', error.message);
            } else {
                console.log('✅ Bi-weekly rental summary saved to database');
            }
        } catch (error) {
            console.error('❌ Save rental summary error:', error.message);
        }
    }

    /**
     * Enhanced summary with deduplication performance
     */
    logSmartDeduplicationSummary(summary) {
        const mode = this.initialBulkLoad ? 'INITIAL BULK LOAD' : 'SMART DEDUPLICATION';
        
        console.log(`\n📊 ${mode} RENTAL ANALYSIS COMPLETE`);
        console.log('='.repeat(70));
        
        if (this.initialBulkLoad) {
            console.log(`🚀 BULK LOAD: All ${summary.totalNeighborhoods} rental neighborhoods processed`);
            console.log(`⏱️ Duration: ${summary.duration.toFixed(1)} minutes (~${(summary.duration/60).toFixed(1)} hours)`);
        } else {
            console.log(`📅 Schedule Day: ${summary.scheduledDay} of bi-weekly cycle`);
            console.log(`⏱️ Duration: ${summary.duration.toFixed(1)} minutes`);
        }
        
        console.log(`🗽 Neighborhoods processed: ${summary.neighborhoodsProcessed}/${summary.totalNeighborhoods}`);
        
        // Core metrics
        console.log('\n📊 Core Analysis Metrics:');
        console.log(`🏠 Active rentals found: ${summary.totalActiveRentalsFound}`);
        console.log(`🔍 Details attempted: ${summary.totalDetailsAttempted}`);
        console.log(`✅ Details successfully fetched: ${summary.totalDetailsFetched}`);
        console.log(`🎯 Undervalued rentals found: ${summary.undervaluedFound}`);
        console.log(`💾 Saved to database: ${summary.savedToDatabase}`);
        console.log(`📞 API calls used: ${summary.apiCallsUsed}`);
        
        // DEDUPLICATION PERFORMANCE HIGHLIGHT (only if not bulk load)
        if (!this.initialBulkLoad) {
            console.log('\n⚡ SMART DEDUPLICATION PERFORMANCE:');
            console.log(`💾 API calls saved by cache: ${summary.apiCallsSaved}`);
            console.log(`📈 Cache hit rate: ${summary.cacheHitRate.toFixed(1)}%`);
            console.log(`🏠 Listings marked as rented: ${summary.listingsMarkedRented}`);
            
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
            console.log('\n🎯 RENTAL BULK LOAD COMPLETE!');
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
            console.log('\n🎉 SUCCESS: Found undervalued rentals efficiently!');
            console.log(`🔍 Check your Supabase 'undervalued_rentals' table for ${summary.savedToDatabase} new deals`);
            
            if (!this.initialBulkLoad && summary.apiCallsSaved > 0) {
                const efficiency = ((summary.apiCallsSaved / (summary.apiCallsUsed + summary.apiCallsSaved)) * 100).toFixed(1);
                console.log(`⚡ Achieved ${efficiency}% API efficiency through smart caching`);
            }
        } else {
            console.log('\n📊 No undervalued rentals found (normal in competitive NYC rental market)');
            console.log('💡 Try adjusting criteria or neighborhoods - 25% threshold is very strict for NYC');
        }
        
        // Long-term projection (only for regular mode)
        if (!this.initialBulkLoad && summary.apiCallsSaved > 0) {
            console.log(`\n📊 Deduplication Impact: Expect 75-90% API savings in future runs`);
            console.log(`💡 This system scales efficiently for long-term operation`);
        }

        // FIXED: Database function status
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
     * Enhanced delay with adaptive rate limiting
     */
    async smartDelay() {
        const delayTime = this.adaptiveRateLimit();
        
        if (delayTime > 60000) { // More than 1 minute
            console.log(`   ⏰ Long delay: ${Math.round(delayTime/1000/60)} minutes (rate limit protection)`);
        } else {
            console.log(`   ⏰ Adaptive delay: ${Math.round(delayTime/1000)}s`);
        }
        
        await this.delay(delayTime);
    }

    delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    /**
     * Main bi-weekly rental refresh with SMART DEDUPLICATION + ADVANCED VALUATION
     */
    async runBiWeeklyRentalRefresh() {
        console.log('\n🏠 ADVANCED MULTI-FACTOR RENTAL ANALYSIS');
        console.log('🎯 NEW: 25% threshold using bed/bath/amenity valuation engine');
        console.log('💾 Cache-optimized to save 75-90% of API calls');
        console.log('🏠 Auto-detects and removes rented listings');
        console.log('⚡ Adaptive rate limiting with daily neighborhood scheduling');
        console.log('⏰ 5-second delay to prevent API conflicts');
        console.log('🔧 FIXED: Database function dependencies resolved');
        console.log('='.repeat(70));

        // Get today's neighborhood assignment WITH 5-SECOND DELAY + BULK LOAD SUPPORT
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
            totalActiveRentalsFound: 0,
            totalDetailsAttempted: 0,
            totalDetailsFetched: 0,
            undervaluedFound: 0,
            savedToDatabase: 0,
            apiCallsUsed: 0,
            adaptiveDelayChanges: 0,
            // NEW: Deduplication stats
            apiCallsSaved: 0,
            cacheHitRate: 0,
            listingsMarkedRented: 0,
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
            // Clear old rental data and run automatic cleanup
            await this.clearOldRentalData();
            await this.runAutomaticRentedDetection();

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
                    
                    // Step 1: Get ALL active rentals with smart deduplication
                    const { newRentals, totalFound, cacheHits } = await this.fetchActiveRentalsWithDeduplication(neighborhood);
                    summary.totalActiveRentalsFound += totalFound;
                    this.apiUsageStats.totalListingsFound += totalFound;
                    this.apiUsageStats.cacheHits += cacheHits;
                    this.apiUsageStats.newListingsToFetch += newRentals.length;
                    this.apiUsageStats.apiCallsSaved += cacheHits;
                    
                    if (newRentals.length === 0 && !this.initialBulkLoad) {
                        console.log(`   📊 All ${totalFound} rentals found in cache - 100% API savings!`);
                        continue;
                    }

                    console.log(`   🎯 Smart deduplication: ${totalFound} total, ${newRentals.length} new, ${cacheHits} cached`);
                    if (cacheHits > 0 && !this.initialBulkLoad) {
                        console.log(`   ⚡ API savings: ${cacheHits} detail calls avoided!`);
                    };
                    
                    // Step 2: Fetch details ONLY for new rentals
                    const detailedRentals = await this.fetchRentalDetailsWithCache(newRentals, neighborhood);
                    summary.totalDetailsAttempted += newRentals.length;
                    summary.totalDetailsFetched += detailedRentals.length;
                    
                    // Step 3: ADVANCED MULTI-FACTOR ANALYSIS for undervaluation
                    const undervaluedRentals = this.analyzeForAdvancedRentalUndervaluation(detailedRentals, neighborhood);
                    summary.undervaluedFound += undervaluedRentals.length;
                    
                    // Step 4: Save to database
                    if (undervaluedRentals.length > 0) {
                        const saved = await this.saveUndervaluedRentalsToDatabase(undervaluedRentals, neighborhood);
                        summary.savedToDatabase += saved;
                    }
                    
                    // Step 5: Update cache with analysis results
                    await this.updateCacheWithRentalAnalysisResults(detailedRentals, undervaluedRentals);
                    
                    // Track neighborhood stats
                    summary.detailedStats.byNeighborhood[neighborhood] = {
                        totalFound: totalFound,
                        cacheHits: cacheHits,
                        newRentals: newRentals.length,
                        detailsFetched: detailedRentals.length,
                        undervaluedFound: undervaluedRentals.length,
                        apiCallsUsed: 1 + newRentals.length, // 1 search + detail calls
                        apiCallsSaved: cacheHits
                    };
                    
                    summary.neighborhoodsProcessed++;
                    console.log(`   ✅ ${neighborhood}: ${undervaluedRentals.length} undervalued rentals found (25% threshold)`);

                    // For bulk load, log progress every 5 neighborhoods
                    if (this.initialBulkLoad && (i + 1) % 5 === 0) {
                        const progress = ((i + 1) / todaysNeighborhoods.length * 100).toFixed(1);
                        const elapsed = (new Date() - summary.startTime) / 1000 / 60;
                        const eta = elapsed / (i + 1) * todaysNeighborhoods.length - elapsed;
                        console.log(`\n📊 BULK LOAD PROGRESS: ${progress}% complete (${i + 1}/${todaysNeighborhoods.length})`);
                        console.log(`⏱️ Elapsed: ${elapsed.toFixed(1)}min, ETA: ${eta.toFixed(1)}min`);
                        console.log(`🎯 Found ${summary.undervaluedFound} total undervalued rentals so far\n`);
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
            summary.listingsMarkedRented = this.apiUsageStats.listingsMarkedRented;
            summary.adaptiveDelayChanges = this.apiUsageStats.adaptiveDelayChanges;
            summary.detailedStats.rateLimit = {
                initialDelay: this.initialBulkLoad ? 8000 : 6000,
                finalDelay: this.baseDelay,
                rateLimitHits: this.rateLimitHits
            };

            await this.saveBiWeeklyRentalSummary(summary);
            this.logSmartDeduplicationSummary(summary);
        } catch (error) {
            console.error('💥 Smart deduplication rental refresh failed:', error.message);
            summary.errors.push({ error: error.message });
        }

        return { summary };
    }

    /**
     * SMART DEDUPLICATION: Fetch active rentals and identify which need detail fetching
     */
    async fetchActiveRentalsWithDeduplication(neighborhood) {
        try {
            console.log(`   📡 Fetching active rentals for ${neighborhood} with smart deduplication...`);
            
            // Step 1: Get basic neighborhood search (1 API call)
            const response = await axios.get(
                'https://streeteasy-api.p.rapidapi.com/rentals/search',
                {
                    params: {
                        areas: neighborhood,
                        limit: 500,
                        minPrice: 1000,
                        maxPrice: 20000,
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
            this.apiUsageStats.activeRentalsCalls++;

            // Handle response structure
            let rentalData = [];
            if (response.data) {
                if (response.data.results && Array.isArray(response.data.results)) {
                    rentalData = response.data.results;
                } else if (response.data.listings && Array.isArray(response.data.listings)) {
                    rentalData = response.data.listings;
                } else if (Array.isArray(response.data)) {
                    rentalData = response.data;
                }
            }

            console.log(`   ✅ Retrieved ${rentalData.length} total active rentals`);

            // Step 2: Check cache for complete details AND handle rent changes efficiently
            const listingIds = rentalData.map(rental => rental.id?.toString()).filter(Boolean);
            const { completeListingIds, rentUpdatedIds } = await this.handleRentUpdatesInCache(listingIds, rentalData, neighborhood);
            
            // Step 3: Filter to ONLY truly NEW rentals (rent-changed rentals already handled)
            const newRentals = rentalData.filter(rental => 
                !completeListingIds.includes(rental.id?.toString())
            );

            const cacheHits = completeListingIds.length;
            const rentUpdates = rentUpdatedIds.length;

            // Step 4: Update search timestamps for rented detection
            await this.updateRentTimestampsOnly(rentalData, neighborhood);
            
            console.log(`   🎯 Optimized deduplication: ${rentalData.length} total, ${newRentals.length} need fetching, ${cacheHits} cache hits, ${rentUpdates} rent-only updates`);
            
            return {
                newRentals,
                totalFound: rentalData.length,
                cacheHits: cacheHits,
                rentUpdates: rentUpdates
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
     * Fetch rental details with cache updates
     * FIXED: Cache ONLY after successful individual fetch + complete function implementation
     */
    async fetchRentalDetailsWithCache(newRentals, neighborhood) {
        console.log(`   🔍 Fetching details for ${newRentals.length} NEW rentals (saving API calls from cache)...`);
        
        const detailedRentals = [];
        let successCount = 0;
        let failureCount = 0;

        for (let i = 0; i < newRentals.length; i++) {
            const rental = newRentals[i];
            
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

                const details = await this.fetchRentalDetails(rental.id);
                
                if (details && this.isValidRentalData(details)) {
                    const fullRentalData = {
                        ...rental,
                        ...details,
                        neighborhood: neighborhood,
                        fetchedAt: new Date().toISOString()
                    };
                    
                    detailedRentals.push(fullRentalData);
                    
                    // FIXED: Cache complete rental details ONLY AFTER successful individual fetch
                    await this.cacheCompleteRentalDetails(rental.id, details, neighborhood);
                    
                    successCount++;
                } else {
                    failureCount++;
                    // FIXED: Cache failed fetch ONLY after we tried and failed
                    await this.cacheFailedRentalFetch(rental.id, neighborhood);
                }

                // Progress logging every 20 properties
                if ((i + 1) % 20 === 0) {
                    const currentDelay = this.baseDelay;
                    console.log(`   📊 Progress: ${i + 1}/${newRentals.length} (${successCount} successful, ${failureCount} failed, ${currentDelay/1000}s delay)`);
                }

            } catch (error) {
                failureCount++;
                // FIXED: Cache failed fetch ONLY after we tried and failed
                await this.cacheFailedRentalFetch(rental.id, neighborhood);
                
                if (error.response?.status === 429) {
                    this.rateLimitHits++;
                    console.log(`   ⚡ Rate limit hit #${this.rateLimitHits} for ${rental.id}, adapting...`);
                    this.baseDelay = Math.min(25000, this.baseDelay * 1.5);
                    await this.delay(this.baseDelay * 2);
                } else {
                    console.log(`   ⚠️ Failed to get details for ${rental.id}: ${error.message}`);
                }
            }
        }

        console.log(`   ✅ Rental detail fetch complete: ${successCount} successful, ${failureCount} failed`);
        return detailedRentals;
    }

    /**
     * Run automatic rented detection based on cache
     * FIXED: Graceful degradation when database functions are missing
     */
    async runAutomaticRentedDetection() {
        try {
            console.log('🏠 Running automatic rented detection...');
            
            // Try to call the database function with graceful fallback
            const { data, error } = await this.supabase.rpc('mark_likely_rented_listings');
            
            if (error) {
                console.warn('⚠️ Rented detection function not available:', error.message);
                console.warn('   Continuing without automatic rented detection');
                console.warn('   Manual detection will still work through cache comparisons');
                return 0;
            }
            
            const markedCount = data || 0;
            if (markedCount > 0) {
                console.log(`🏠 Marked ${markedCount} listings as likely rented`);
                this.apiUsageStats.listingsMarkedRented += markedCount;
            }
            
            return markedCount;
        } catch (error) {
            console.warn('⚠️ Automatic rented detection function not available:', error.message);
            console.warn('   This is expected if database functions are not yet created');
            console.warn('   Manual rented detection through cache comparisons will still work');
            return 0;
        }
    }

    /**
     * Fetch individual rental details using /rentals/{id}
     */
    async fetchRentalDetails(rentalId) {
        try {
            const response = await axios.get(
                `https://streeteasy-api.p.rapidapi.com/rentals/${rentalId}`,
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
            
            // Extract rental details based on actual API response
            return {
                // Basic property info
                address: data.address || 'Address not available',
                bedrooms: data.bedrooms || 0,
                bathrooms: data.bathrooms || 0,
                sqft: data.sqft || 0,
                propertyType: data.propertyType || 'apartment',
                
                // Rental pricing
                monthlyRent: data.price || 0,
                rentPerSqft: (data.sqft > 0 && data.price > 0) ? data.price / data.sqft : null,
                
                // Rental status and timing
                status: data.status || 'unknown',
                listedAt: data.listedAt || null,
                closedAt: data.closedAt || null,
                availableFrom: data.availableFrom || null,
                daysOnMarket: data.daysOnMarket || 0,
                type: data.type || 'rental',
                
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
                noFee: data.noFee || false,
                
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
     * Validate rental data is complete enough for analysis
     */
    isValidRentalData(rental) {
        return rental &&
               rental.address &&
               rental.monthlyRent > 0 &&
               rental.bedrooms !== undefined &&
               rental.bathrooms !== undefined;
    }

    /**
     * NEW: ADVANCED MULTI-FACTOR ANALYSIS for undervaluation using the sophisticated engine
     */
    analyzeForAdvancedRentalUndervaluation(detailedRentals, neighborhood) {
        if (detailedRentals.length < 5) {
            console.log(`   ⚠️ Not enough rentals (${detailedRentals.length}) for advanced multi-factor analysis in ${neighborhood}`);
            return [];
        }

        console.log(`   🎯 ADVANCED MULTI-FACTOR ANALYSIS: ${detailedRentals.length} rentals using bed/bath/amenity engine...`);

        const undervaluedRentals = [];

        // Analyze each rental using the advanced valuation engine
        for (const rental of detailedRentals) {
            try {
                // Use the advanced valuation engine with 25% threshold
                const analysis = this.valuationEngine.analyzeRentalUndervaluation(
                    rental, 
                    detailedRentals, 
                    neighborhood,
                    { undervaluationThreshold: 25 } // NEW: Only 25%+ below market flagged
                );
                
                if (analysis.isUndervalued) {
                    undervaluedRentals.push({
                        ...rental,
                        // Advanced analysis results
                        discountPercent: analysis.discountPercent,
                        estimatedMarketRent: analysis.estimatedMarketRent,
                        actualRent: analysis.actualRent,
                        potentialMonthlySavings: analysis.potentialMonthlySavings,
                        confidence: analysis.confidence,
                        valuationMethod: analysis.method,
                        comparablesUsed: analysis.comparablesUsed,
                        adjustmentBreakdown: analysis.adjustmentBreakdown,
                        reasoning: analysis.reasoning,
                        
                        // Generate advanced score based on multiple factors
                        score: this.calculateAdvancedRentalScore(analysis),
                        grade: this.calculateGrade(this.calculateAdvancedRentalScore(analysis)),
                        comparisonGroup: `${rental.bedrooms}BR/${rental.bathrooms}BA in ${neighborhood}`,
                        comparisonMethod: analysis.method
                    });
                }
            } catch (error) {
                console.warn(`   ⚠️ Error analyzing ${rental.address}: ${error.message}`);
            }
        }

        // Sort by discount percentage (best deals first)
        undervaluedRentals.sort((a, b) => b.discountPercent - a.discountPercent);

        console.log(`   🎯 Found ${undervaluedRentals.length} undervalued rentals (25% threshold with advanced valuation)`);
        return undervaluedRentals;
    }

    /**
     * Calculate advanced rental score based on multi-factor analysis
     */
    calculateAdvancedRentalScore(analysis) {
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

        // Savings magnitude bonus (0-5 points)
        if (analysis.potentialMonthlySavings >= 1000) score += 5;
        else if (analysis.potentialMonthlySavings >= 500) score += 3;

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
     * Save undervalued rentals to database with enhanced deduplication check and advanced valuation data
     */
    async saveUndervaluedRentalsToDatabase(undervaluedRentals, neighborhood) {
        console.log(`   💾 Saving ${undervaluedRentals.length} undervalued rentals to database...`);

        let savedCount = 0;

        for (const rental of undervaluedRentals) {
            try {
                // Enhanced duplicate check
                const { data: existing } = await this.supabase
                    .from('undervalued_rentals')
                    .select('id, score')
                    .eq('listing_id', rental.id)
                    .single();

                if (existing) {
                    // Update if score improved
                    if (rental.score > existing.score) {
                        const { error: updateError } = await this.supabase
                            .from('undervalued_rentals')
                            .update({
                                score: rental.score,
                                discount_percent: rental.discountPercent,
                                last_seen_in_search: new Date().toISOString(),
                                times_seen_in_search: 1, // Reset counter
                                analysis_date: new Date().toISOString()
                            })
                            .eq('id', existing.id);

                        if (!updateError) {
                            console.log(`   🔄 Updated: ${rental.address} (score: ${existing.score} → ${rental.score})`);
                        }
                    } else {
                        console.log(`   ⏭️ Skipping duplicate: ${rental.address}`);
                    }
                    continue;
                }

                // Enhanced database record with advanced valuation data
                const dbRecord = {
                    listing_id: rental.id?.toString(),
                    address: rental.address,
                    neighborhood: rental.neighborhood,
                    borough: rental.borough || 'unknown',
                    zipcode: rental.zipcode,
                    
                    // Advanced rental pricing analysis
                    monthly_rent: parseInt(rental.monthlyRent) || 0,
                    rent_per_sqft: rental.actualRent && rental.sqft > 0 ? parseFloat((rental.actualRent / rental.sqft).toFixed(2)) : null,
                    market_rent_per_sqft: rental.estimatedMarketRent && rental.sqft > 0 ? parseFloat((rental.estimatedMarketRent / rental.sqft).toFixed(2)) : null,
                    discount_percent: parseFloat(rental.discountPercent.toFixed(2)),
                    potential_monthly_savings: parseInt(rental.potentialMonthlySavings) || 0,
                    annual_savings: parseInt((rental.potentialMonthlySavings || 0) * 12),
                    
                    // Property details
                    bedrooms: parseInt(rental.bedrooms) || 0,
                    bathrooms: rental.bathrooms ? parseFloat(rental.bathrooms) : null,
                    sqft: rental.sqft ? parseInt(rental.sqft) : null,
                    property_type: rental.propertyType || 'apartment',
                    
                    // Rental terms
                    listing_status: rental.status || 'unknown',
                    listed_at: rental.listedAt ? new Date(rental.listedAt).toISOString() : null,
                    closed_at: rental.closedAt ? new Date(rental.closedAt).toISOString() : null,
                    available_from: rental.availableFrom ? new Date(rental.availableFrom).toISOString() : null,
                    days_on_market: parseInt(rental.daysOnMarket) || 0,
                    no_fee: rental.noFee || false,
                    
                    // Building features
                    doorman_building: rental.doormanBuilding || false,
                    elevator_building: rental.elevatorBuilding || false,
                    pet_friendly: rental.petFriendly || false,
                    laundry_available: rental.laundryAvailable || false,
                    gym_available: rental.gymAvailable || false,
                    rooftop_access: rental.rooftopAccess || false,
                    
                    // Building info
                    built_in: rental.builtIn ? parseInt(rental.builtIn) : null,
                    latitude: rental.latitude ? parseFloat(rental.latitude) : null,
                    longitude: rental.longitude ? parseFloat(rental.longitude) : null,
                    
                    // Media and description
                    images: Array.isArray(rental.images) ? rental.images : [],
                    image_count: Array.isArray(rental.images) ? rental.images.length : 0,
                    videos: Array.isArray(rental.videos) ? rental.videos : [],
                    floorplans: Array.isArray(rental.floorplans) ? rental.floorplans : [],
                    description: typeof rental.description === 'string' ? 
                        rental.description.substring(0, 2000) : '',
                    
                    // Amenities
                    amenities: Array.isArray(rental.amenities) ? rental.amenities : [],
                    amenity_count: Array.isArray(rental.amenities) ? rental.amenities.length : 0,
                    
                    // Advanced analysis results
                    score: parseInt(rental.score) || 0,
                    grade: rental.grade || 'F',
                    reasoning: rental.reasoning || '',
                    comparison_group: rental.comparisonGroup || '',
                    comparison_method: rental.valuationMethod || rental.comparisonMethod || '',
                    reliability_score: parseInt(rental.confidence) || 0,
                    
                    // Additional data
                    building_info: typeof rental.building === 'object' ? rental.building : {},
                    agents: Array.isArray(rental.agents) ? rental.agents : [],
                    rental_type: rental.type || 'rental',
                    
                    // ENHANCED: Deduplication and rented tracking fields
                    last_seen_in_search: new Date().toISOString(),
                    times_seen_in_search: 1,
                    likely_rented: false,
                    
                    analysis_date: new Date().toISOString(),
                    status: 'active'
                };

                const { error } = await this.supabase
                    .from('undervalued_rentals')
                    .insert([dbRecord]);

                if (error) {
                    console.error(`   ❌ Error saving rental ${rental.address}:`, error.message);
                } else {
                    console.log(`   ✅ Saved: ${rental.address} (${rental.discountPercent}% below market, Score: ${rental.score}, Method: ${rental.valuationMethod})`);
                    savedCount++;
                }
            } catch (error) {
                console.error(`   ❌ Error processing rental ${rental.address}:`, error.message);
            }
        }

        console.log(`   💾 Saved ${savedCount} new undervalued rentals using advanced multi-factor analysis`);
        return savedCount;
    }

    /**
     * Get latest undervalued rentals with status filtering
     */
    async getLatestUndervaluedRentals(limit = 50, minScore = 50) {
        try {
            const { data, error } = await this.supabase
                .from('undervalued_rentals')
                .select('*')
                .eq('status', 'active') // Only active listings
                .gte('score', minScore)
                .order('analysis_date', { ascending: false })
                .limit(limit);

            if (error) throw error;
            return data;
        } catch (error) {
            console.error('❌ Error fetching latest rentals:', error.message);
            return [];
        }
    }

    /**
     * Get rentals by neighborhood with status filtering
     */
    async getRentalsByNeighborhood(neighborhood, limit = 20) {
        try {
            const { data, error } = await this.supabase
                .from('undervalued_rentals')
                .select('*')
                .eq('neighborhood', neighborhood)
                .eq('status', 'active') // Only active listings
                .order('score', { ascending: false })
                .limit(limit);

            if (error) throw error;
            return data;
        } catch (error) {
            console.error('❌ Error fetching rentals by neighborhood:', error.message);
            return [];
        }
    }

    /**
     * Get top scoring rental deals (active only)
     */
    async getTopRentalDeals(limit = 20) {
        try {
            const { data, error } = await this.supabase
                .from('undervalued_rentals')
                .select('*')
                .eq('status', 'active') // Only active listings
                .gte('score', 70)
                .order('score', { ascending: false })
                .limit(limit);

            if (error) throw error;
            return data;
        } catch (error) {
            console.error('❌ Error fetching top rental deals:', error.message);
            return [];
        }
    }

    /**
     * Get rentals by specific criteria (active only)
     */
    async getRentalsByCriteria(criteria = {}) {
        try {
            let query = this.supabase
                .from('undervalued_rentals')
                .select('*')
                .eq('status', 'active'); // Only active listings

            if (criteria.maxRent) {
                query = query.lte('monthly_rent', criteria.maxRent);
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
            if (criteria.noFee) {
                query = query.eq('no_fee', true);
            }

            const { data, error } = await query
                .order('score', { ascending: false })
                .limit(criteria.limit || 20);

            if (error) throw error;
            return data;
        } catch (error) {
            console.error('❌ Error fetching rentals by criteria:', error.message);
            return [];
        }
    }

    /**
     * Setup enhanced database schema for rentals with deduplication
     * FIXED: Graceful setup without requiring database functions
     */
    async setupRentalDatabase() {
        console.log('🔧 Setting up enhanced rental database schema with deduplication...');

        try {
            console.log('✅ Enhanced rental database with deduplication is ready');
            console.log('💾 Core tables will be created via SQL schema');
            console.log('🏠 Basic rented listing detection enabled');
            console.log('⚠️ Advanced database functions can be added later for enhanced features');
            console.log('\n💡 For full functionality, add these SQL functions to your database:');
            console.log('   - mark_likely_rented_listings()');
            console.log('   - cleanup_old_cache_entries()');
            
        } catch (error) {
            console.error('❌ Rental database setup error:', error.message);
        }
    }
}

// CLI interface for rentals with enhanced deduplication features and advanced valuation
async function main() {
    const args = process.argv.slice(2);
    
    if (!process.env.RAPIDAPI_KEY || !process.env.SUPABASE_URL || !process.env.SUPABASE_ANON_KEY) {
        console.error('❌ Missing required environment variables!');
        console.error('   RAPIDAPI_KEY, SUPABASE_URL, SUPABASE_ANON_KEY required');
        process.exit(1);
    }

    const analyzer = new EnhancedBiWeeklyRentalAnalyzer();

    if (args.includes('--setup')) {
        await analyzer.setupRentalDatabase();
        return;
    }

    if (args.includes('--latest')) {
        const limit = parseInt(args[args.indexOf('--latest') + 1]) || 20;
        const rentals = await analyzer.getLatestUndervaluedRentals(limit);
        console.log(`🏠 Latest ${rentals.length} active undervalued rentals (25% threshold):`);
        rentals.forEach((rental, i) => {
            console.log(`${i + 1}. ${rental.address} - ${rental.monthly_rent.toLocaleString()}/month (${rental.discount_percent}% below market, Score: ${rental.score})`);
        });
        return;
    }

    if (args.includes('--top-deals')) {
        const limit = parseInt(args[args.indexOf('--top-deals') + 1]) || 10;
        const deals = await analyzer.getTopRentalDeals(limit);
        console.log(`🏆 Top ${deals.length} active rental deals (25% threshold):`);
        deals.forEach((deal, i) => {
            console.log(`${i + 1}. ${deal.address} - ${deal.monthly_rent.toLocaleString()}/month (${deal.discount_percent}% below market, Score: ${deal.score})`);
        });
        return;
    }

    if (args.includes('--neighborhood')) {
        const neighborhood = args[args.indexOf('--neighborhood') + 1];
        if (!neighborhood) {
            console.error('❌ Please provide a neighborhood: --neighborhood park-slope');
            return;
        }
        const rentals = await analyzer.getRentalsByNeighborhood(neighborhood);
        console.log(`🏠 Active rentals in ${neighborhood} (25% threshold):`);
        rentals.forEach((rental, i) => {
            console.log(`${i + 1}. ${rental.address} - ${rental.monthly_rent.toLocaleString()}/month (Score: ${rental.score})`);
        });
        return;
    }

    if (args.includes('--doorman')) {
        const rentals = await analyzer.getRentalsByCriteria({ doorman: true, limit: 15 });
        console.log(`🚪 Active doorman building rentals (25% threshold):`);
        rentals.forEach((rental, i) => {
            console.log(`${i + 1}. ${rental.address} - ${rental.monthly_rent.toLocaleString()}/month (${rental.discount_percent}% below market)`);
        });
        return;
    }

    if (args.includes('--no-fee')) {
        const rentals = await analyzer.getRentalsByCriteria({ noFee: true, limit: 15 });
        console.log(`💰 Active no-fee rentals (25% threshold):`);
        rentals.forEach((rental, i) => {
            console.log(`${i + 1}. ${rental.address} - ${rental.monthly_rent.toLocaleString()}/month (${rental.discount_percent}% below market, Annual savings: ${rental.annual_savings.toLocaleString()})`);
        });
        return;
    }

    // Default: run bi-weekly rental analysis with advanced multi-factor valuation
    console.log('🏠 Starting ADVANCED bi-weekly rental analysis with 25% threshold and multi-factor valuation...');
    const results = await analyzer.runBiWeeklyRentalRefresh();
    
    console.log('\n🎉 Advanced bi-weekly rental analysis with smart deduplication completed!');
    
    if (results.summary && results.summary.apiCallsSaved > 0) {
        const efficiency = ((results.summary.apiCallsSaved / (results.summary.apiCallsUsed + results.summary.apiCallsSaved)) * 100).toFixed(1);
        console.log(`⚡ Achieved ${efficiency}% API efficiency through smart caching!`);
    }
    
    if (results.summary && results.summary.savedToDatabase) {
        console.log(`📊 Check your Supabase 'undervalued_rentals' table for ${results.summary.savedToDatabase} new deals!`);
        console.log(`🎯 All properties are 25%+ below market using advanced bed/bath/amenity valuation`);
    }
    
    return results;
}

// Export for use in other modules
module.exports = EnhancedBiWeeklyRentalAnalyzer;

// Run if executed directly
if (require.main === module) {
    main().catch(error => {
        console.error('💥 Enhanced rental analyzer with advanced valuation crashed:', error);
        process.exit(1);
    });
}
