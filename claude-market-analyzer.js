// claude-market-analyzer.js
// ENHANCED CLAUDE-POWERED MARKET ANALYSIS ENGINE - PRODUCTION-READY VERSION
// Supports all 3 table types: undervalued_rentals, undervalued_rent_stabilized, undervalued_sales
// Combines hierarchical comparable filtering with natural Claude AI analysis
// 
// DEPENDENCIES REQUIRED:
// npm install axios dotenv
//
// ENVIRONMENT VARIABLES REQUIRED:
// ANTHROPIC_API_KEY=your_claude_api_key
//
// RAILWAY DEPLOYMENT READY: All field mappings verified against database constraints
require('dotenv').config();
const axios = require('axios');

/**
 * Enhanced Claude-Powered Market Analysis Engine
 * Complete system with pre-filtering + Claude AI for all property types
 */
class EnhancedClaudeMarketAnalyzer {
    constructor() {
        this.claudeApiKey = process.env.ANTHROPIC_API_KEY || process.env.CLAUDE_API_KEY;
        this.apiCallsUsed = 0;
        this.cacheTimeout = 3600000; // 1 hour cache for neighborhood analysis
        this.neighborhoodCache = new Map();
        
        if (!this.claudeApiKey) {
            throw new Error('ANTHROPIC_API_KEY or CLAUDE_API_KEY environment variable is required');
        }
        
        console.log('🤖 Enhanced Claude Market Analyzer initialized (All Tables Support)');
    }

    /**
     * ENHANCED RENTALS ANALYSIS - SIMPLIFIED VERSION (matches working pattern)
     */
    async analyzeRentalsUndervaluation(targetProperty, comparableProperties, neighborhood, options = {}) {
        const threshold = options.undervaluationThreshold || 15;
        
        console.log(`🤖 Claude analyzing rental: ${targetProperty.address}`);
        
        try {
            // STEP 1: Pre-filter comparables using hierarchy (KEEP THIS - it works)
            const filteredComparables = this.filterComparablesUsingHierarchy(targetProperty, comparableProperties);
            console.log(`   🎯 Filtered to ${filteredComparables.selectedComparables.length} specific matches using ${filteredComparables.method}`);
            
            // STEP 2: Build context with filtered comparables for Claude
            const enhancedContext = this.buildEnhancedRentalsContext(targetProperty, filteredComparables.selectedComparables, neighborhood, options);
            
            // STEP 3: Let Claude analyze the specific comparables naturally
            const claudeResponse = await this.callClaudeForEnhancedRentalsAnalysis(enhancedContext, threshold);
            
            if (!claudeResponse.success) {
                return {
                    isUndervalued: false,
                    percentBelowMarket: 0,
                    estimatedMarketRent: targetProperty.price,
                    actualRent: targetProperty.price,
                    confidence: 0,
                    method: 'claude_analysis_failed',
                    reasoning: claudeResponse.error || 'Analysis failed',
                    rentStabilizedProbability: 0,
                    rentStabilizedFactors: [],
                    rentStabilizedExplanation: 'Analysis failed'
                };
            }
            
            const analysis = claudeResponse.analysis;
            
            // STEP 4: Simple validation (like working version)
            if (!analysis.estimatedMarketRent || !analysis.percentBelowMarket) {
                throw new Error('Invalid analysis structure from Claude');
            }
            
            // STEP 5: Calculate confidence from method (since Claude doesn't provide it)
            const calculatedConfidence = this.calculateConfidenceFromMethod(filteredComparables.method, filteredComparables.selectedComparables.length);
            
            console.log(`   💰 Claude estimate: ${analysis.estimatedMarketRent?.toLocaleString()}/month`);
            console.log(`   📊 Below market: ${analysis.percentBelowMarket?.toFixed(1)}%`);
            if (analysis.rentStabilizedProbability >= 60) {
                console.log(`   🔒 Rent stabilized: ${analysis.rentStabilizedProbability}%`);
            }
            
            // STEP 6: Return SIMPLE structure (like working version)
            return {
                isUndervalued: analysis.percentBelowMarket >= threshold && calculatedConfidence >= 60,
                percentBelowMarket: analysis.percentBelowMarket || 0,
                estimatedMarketRent: analysis.estimatedMarketRent || targetProperty.price,
                actualRent: targetProperty.price,
                potentialSavings: analysis.potentialSavings || 0,
                confidence: calculatedConfidence,
                method: 'claude_hierarchical_analysis',
                comparablesUsed: filteredComparables.selectedComparables.length,
                reasoning: analysis.reasoning || 'Claude AI market analysis',
                undervaluationConfidence: calculatedConfidence,
                
                // Rent stabilization analysis
                rentStabilizedProbability: analysis.rentStabilizedProbability || 0,
                rentStabilizedFactors: analysis.rentStabilizedFactors || [],
                rentStabilizedExplanation: analysis.rentStabilizedExplanation || 'No stabilization indicators found',
                
                // Enhanced metrics for compatibility
                detailedAnalysis: analysis.detailedAnalysis || {},
                valuationMethod: filteredComparables.method,
                baseMarketRent: analysis.baseMarketRent || analysis.estimatedMarketRent,
                adjustmentBreakdown: analysis.adjustmentBreakdown || {},
                legalProtectionValue: analysis.rentStabilizedProbability >= 60 ? (analysis.potentialSavings || 0) * 12 : 0,
                investmentMerit: analysis.percentBelowMarket >= 25 ? 'strong' : analysis.percentBelowMarket >= 15 ? 'moderate' : 'low'
            };
            
        } catch (error) {
            console.warn(`   ⚠️ Claude analysis error: ${error.message}`);
            return {
                isUndervalued: false,
                percentBelowMarket: 0,
                estimatedMarketRent: targetProperty.price,
                actualRent: targetProperty.price,
                confidence: 0,
                method: 'claude_analysis_failed',
                reasoning: `Analysis failed: ${error.message}`,
                comparablesUsed: 0,
                undervaluationConfidence: 0,
                rentStabilizedProbability: 0,
                rentStabilizedFactors: [],
                rentStabilizedExplanation: 'Analysis failed'
            };
        }
    }

    /**
     * ENHANCED SALES ANALYSIS - SIMPLIFIED VERSION (matches working pattern)
     */
    async analyzeSalesUndervaluation(targetProperty, comparableProperties, neighborhood, options = {}) {
        const threshold = options.undervaluationThreshold || 10;
        
        console.log(`🤖 Claude analyzing sale: ${targetProperty.address}`);
        
        try {
            // STEP 1: Pre-filter comparables using hierarchy (adapted for sales)
            const filteredComparables = this.filterSalesComparablesUsingHierarchy(targetProperty, comparableProperties);
            console.log(`   🎯 Filtered to ${filteredComparables.selectedComparables.length} specific matches using ${filteredComparables.method}`);
            
            // STEP 2: Build context with filtered comparables for Claude
            const enhancedContext = this.buildEnhancedSalesContext(targetProperty, filteredComparables.selectedComparables, neighborhood, options);
            
            // STEP 3: Let Claude analyze the specific comparables naturally
            const claudeResponse = await this.callClaudeForEnhancedSalesAnalysis(enhancedContext, threshold);
            
            if (!claudeResponse.success) {
                return {
                    isUndervalued: false,
                    discountPercent: 0,
                    estimatedMarketPrice: targetProperty.salePrice || targetProperty.price,
                    actualPrice: targetProperty.salePrice || targetProperty.price,
                    confidence: 0,
                    method: 'claude_analysis_failed',
                    reasoning: claudeResponse.error || 'Analysis failed',
                    comparablesUsed: 0
                };
            }
            
            const analysis = claudeResponse.analysis;
            
            // STEP 4: Simple validation (like working version)
            if (!analysis.estimatedMarketPrice || !analysis.discountPercent) {
                throw new Error('Invalid analysis structure from Claude');
            }
            
            // STEP 5: Calculate confidence from method (since Claude doesn't provide it)
            const calculatedConfidence = this.calculateConfidenceFromMethod(filteredComparables.method, filteredComparables.selectedComparables.length);
            
            console.log(`   💰 Claude estimate: ${analysis.estimatedMarketPrice?.toLocaleString()}`);
            console.log(`   📊 Below market: ${analysis.discountPercent?.toFixed(1)}%`);
            
            // STEP 6: Return SIMPLE structure (like working version)
            return {
                isUndervalued: analysis.discountPercent >= threshold && calculatedConfidence >= 60,
                discountPercent: analysis.discountPercent || 0,
                estimatedMarketPrice: analysis.estimatedMarketPrice || targetProperty.salePrice || targetProperty.price,
                actualPrice: targetProperty.salePrice || targetProperty.price,
                potentialSavings: (analysis.estimatedMarketPrice || 0) - (targetProperty.salePrice || targetProperty.price || 0),
                confidence: calculatedConfidence,
                method: 'claude_hierarchical_analysis',
                comparablesUsed: filteredComparables.selectedComparables.length,
                reasoning: analysis.reasoning || 'Claude AI market analysis',
                
                // Enhanced metrics for compatibility
                detailedAnalysis: analysis.detailedAnalysis || {},
                adjustmentBreakdown: analysis.adjustmentBreakdown || {},
                valuationMethod: filteredComparables.method
            };
            
        } catch (error) {
            console.warn(`   ⚠️ Claude sales analysis error: ${error.message}`);
            return {
                isUndervalued: false,
                discountPercent: 0,
                estimatedMarketPrice: targetProperty.salePrice || targetProperty.price,
                actualPrice: targetProperty.salePrice || targetProperty.price,
                confidence: 0,
                method: 'claude_analysis_failed',
                reasoning: `Analysis failed: ${error.message}`,
                comparablesUsed: 0
            };
        }
    }

    /**
     * Filter comparables using the original hierarchy approach for RENTALS
     */
    filterComparablesUsingHierarchy(targetProperty, allComparables) {
        const targetBeds = targetProperty.bedrooms || 0;
        const targetBaths = targetProperty.bathrooms || 0;
        const targetAmenities = this.normalizeAmenities(targetProperty.amenities || []);
        
        // Method 1: Try exact bed/bath/amenity match (minimum 3)
        let exactMatches = allComparables.filter(comp => 
            comp.bedrooms === targetBeds && 
            Math.abs(comp.bathrooms - targetBaths) <= 0.5 &&
            this.hasSignificantAmenityOverlap(comp.amenities || [], targetAmenities)
        );
        
        if (exactMatches.length >= 3) {
            return {
                selectedComparables: exactMatches,
                method: 'exact_bed_bath_amenity_match',
                count: exactMatches.length
            };
        }
        
        // Method 2: Same bed/bath with amenity adjustments (minimum 8)
        let bedBathMatches = allComparables.filter(comp => 
            comp.bedrooms === targetBeds && 
            Math.abs(comp.bathrooms - targetBaths) <= 0.5
        );
        
        if (bedBathMatches.length >= 8) {
            return {
                selectedComparables: bedBathMatches,
                method: 'bed_bath_specific_pricing',
                count: bedBathMatches.length
            };
        }
        
        // Method 3: Same bedrooms with bath/amenity adjustments (minimum 12)
        let bedroomMatches = allComparables.filter(comp => comp.bedrooms === targetBeds);
        
        if (bedroomMatches.length >= 12) {
            return {
                selectedComparables: bedroomMatches,
                method: 'bed_specific_with_adjustments',
                count: bedroomMatches.length
            };
        }
        
        // Method 4: Price per sqft fallback (use all comparables)
        return {
            selectedComparables: allComparables,
            method: 'price_per_sqft_fallback',
            count: allComparables.length
        };
    }

    /**
     * Filter comparables using the original hierarchy approach for SALES
     */
    filterSalesComparablesUsingHierarchy(targetProperty, allComparables) {
        const targetBeds = targetProperty.bedrooms || 0;
        const targetBaths = targetProperty.bathrooms || 0;
        const targetAmenities = this.normalizeAmenities(targetProperty.amenities || []);
        
        // Method 1: Try exact bed/bath/amenity match (minimum 3)
        let exactMatches = allComparables.filter(comp => 
            comp.bedrooms === targetBeds && 
            Math.abs(comp.bathrooms - targetBaths) <= 0.5 &&
            this.hasSignificantAmenityOverlap(comp.amenities || [], targetAmenities)
        );
        
        if (exactMatches.length >= 3) {
            return {
                selectedComparables: exactMatches,
                method: 'exact_bed_bath_amenity_match',
                count: exactMatches.length
            };
        }
        
        // Method 2: Same bed/bath with amenity adjustments (minimum 8)
        let bedBathMatches = allComparables.filter(comp => 
            comp.bedrooms === targetBeds && 
            Math.abs(comp.bathrooms - targetBaths) <= 0.5
        );
        
        if (bedBathMatches.length >= 8) {
            return {
                selectedComparables: bedBathMatches,
                method: 'bed_bath_specific_pricing',
                count: bedBathMatches.length
            };
        }
        
        // Method 3: Same bedrooms with bath/amenity adjustments (minimum 12)
        let bedroomMatches = allComparables.filter(comp => comp.bedrooms === targetBeds);
        
        if (bedroomMatches.length >= 12) {
            return {
                selectedComparables: bedroomMatches,
                method: 'bed_specific_with_adjustments',
                count: bedroomMatches.length
            };
        }
        
        // Method 4: Price per sqft fallback (use all comparables)
        return {
            selectedComparables: allComparables,
            method: 'price_per_sqft_fallback',
            count: allComparables.length
        };
    }

    /**
     * DATABASE FIELD MAPPING FUNCTIONS
     */

    /**
     * Map rentals response to correct database structure based on table type
     */
    mapRentalsResponseToDatabase(analysis, targetProperty, filteredComparables, threshold, tableType) {
        const baseResponse = {
            isUndervalued: analysis.percentBelowMarket >= threshold,
            percentBelowMarket: analysis.percentBelowMarket || 0,
            estimatedMarketRent: analysis.estimatedMarketRent || targetProperty.price,
            actualRent: targetProperty.price,
            potentialSavings: analysis.potentialSavings || 0,
            reasoning: analysis.reasoning || 'Claude AI market analysis'
        };

        if (tableType === 'undervalued_rent_stabilized') {
            // Fields for undervalued_rent_stabilized table - EXACT DATABASE FIELD NAMES
            return {
                ...baseResponse,
                method: 'claude_hierarchical_analysis',
                
                // ✅ FIXED: Exact database field names (snake_case)
                comparables_used: filteredComparables.selectedComparables.length,
                undervaluation_confidence: this.calculateConfidenceFromMethod(filteredComparables.method, filteredComparables.selectedComparables.length),
                undervaluation_method: filteredComparables.method, // Required field
                
                // ✅ FIXED: Rent stabilization fields with exact database names
                rent_stabilized_confidence: analysis.rentStabilizedProbability || 0,
                rent_stabilized_method: this.mapRentStabilizedMethod(analysis.rentStabilizedFactors || []),
                
                // Keep camelCase for backward compatibility with existing code
                rentStabilizedProbability: analysis.rentStabilizedProbability || 0,
                rentStabilizedFactors: analysis.rentStabilizedFactors || [],
                rentStabilizedExplanation: analysis.rentStabilizedExplanation || 'No stabilization indicators found',
                comparablesUsed: filteredComparables.selectedComparables.length,
                undervaluationConfidence: this.calculateConfidenceFromMethod(filteredComparables.method, filteredComparables.selectedComparables.length),
                confidence: this.calculateConfidenceFromMethod(filteredComparables.method, filteredComparables.selectedComparables.length),
                
                // Enhanced metrics
                detailedAnalysis: analysis.detailedAnalysis || {},
                valuationMethod: filteredComparables.method,
                baseMarketRent: analysis.baseMarketRent || analysis.estimatedMarketRent,
                adjustmentBreakdown: analysis.adjustmentBreakdown || {},
                legalProtectionValue: analysis.rentStabilizedProbability >= 60 ? (analysis.potentialSavings || 0) * 12 : 0,
                investmentMerit: analysis.percentBelowMarket >= 25 ? 'strong' : analysis.percentBelowMarket >= 15 ? 'moderate' : 'low'
            };
        } else {
            // Fields for undervalued_rentals table
            return {
                ...baseResponse,
                score: this.calculateScoreFromAnalysis(analysis, filteredComparables),
                grade: this.calculateGradeFromScore(this.calculateScoreFromAnalysis(analysis, filteredComparables)),
                comparison_method: this.mapMethodToComparisonMethod(filteredComparables.method),
                reliability_score: this.calculateConfidenceFromMethod(filteredComparables.method, filteredComparables.selectedComparables.length),
                
                // For compatibility
                method: 'claude_hierarchical_analysis',
                comparablesUsed: filteredComparables.selectedComparables.length,
                rentStabilizedProbability: analysis.rentStabilizedProbability || 0,
                rentStabilizedFactors: analysis.rentStabilizedFactors || [],
                rentStabilizedExplanation: analysis.rentStabilizedExplanation || 'No stabilization indicators found'
            };
        }
    }

    /**
     * Map sales response to database structure
     */
    mapSalesResponseToDatabase(analysis, targetProperty, filteredComparables, threshold) {
        return {
            isUndervalued: analysis.discountPercent >= threshold,
            discountPercent: analysis.discountPercent || 0,
            estimatedMarketPrice: analysis.estimatedMarketPrice || targetProperty.salePrice || targetProperty.price,
            actualPrice: targetProperty.salePrice || targetProperty.price,
            potentialSavings: (analysis.estimatedMarketPrice || 0) - (targetProperty.salePrice || targetProperty.price || 0),
            
            // Database fields for undervalued_sales
            score: this.calculateScoreFromSalesAnalysis(analysis, filteredComparables),
            grade: this.calculateGradeFromScore(this.calculateScoreFromSalesAnalysis(analysis, filteredComparables)),
            reasoning: analysis.reasoning || 'Claude AI market analysis',
            comparison_method: this.mapMethodToComparisonMethod(filteredComparables.method),
            reliability_score: this.calculateConfidenceFromMethod(filteredComparables.method, filteredComparables.selectedComparables.length),
            
            // For compatibility
            method: 'claude_hierarchical_analysis',
            comparablesUsed: filteredComparables.selectedComparables.length,
            confidence: this.calculateConfidenceFromMethod(filteredComparables.method, filteredComparables.selectedComparables.length)
        };
    }

    /**
     * SCORING AND GRADING FUNCTIONS
     */

    /**
     * Calculate score from rentals analysis (0-100)
     */
    calculateScoreFromAnalysis(analysis, filteredComparables) {
        let score = 50; // Base score
        
        // Undervaluation bonus (0-40 points)
        const percentBelow = analysis.percentBelowMarket || 0;
        if (percentBelow >= 30) score += 40;
        else if (percentBelow >= 20) score += 30;
        else if (percentBelow >= 15) score += 20;
        else if (percentBelow >= 10) score += 10;
        
        // Method quality bonus (0-20 points)
        switch (filteredComparables.method) {
            case 'exact_bed_bath_amenity_match': score += 20; break;
            case 'bed_bath_specific_pricing': score += 15; break;
            case 'bed_specific_with_adjustments': score += 10; break;
            case 'price_per_sqft_fallback': score += 5; break;
        }
        
        // Sample size bonus (0-10 points)
        const sampleSize = filteredComparables.selectedComparables.length;
        if (sampleSize >= 20) score += 10;
        else if (sampleSize >= 15) score += 7;
        else if (sampleSize >= 10) score += 5;
        else if (sampleSize >= 5) score += 3;
        
        // Rent stabilization bonus (0-10 points)
        const rentStabilizedProb = analysis.rentStabilizedProbability || 0;
        if (rentStabilizedProb >= 80) score += 10;
        else if (rentStabilizedProb >= 60) score += 7;
        else if (rentStabilizedProb >= 40) score += 3;
        
        return Math.min(100, Math.max(0, Math.round(score)));
    }

    /**
     * Calculate score from sales analysis (0-100)
     */
    calculateScoreFromSalesAnalysis(analysis, filteredComparables) {
        let score = 50; // Base score
        
        // Undervaluation bonus (0-40 points)
        const discountPercent = analysis.discountPercent || 0;
        if (discountPercent >= 25) score += 40;
        else if (discountPercent >= 20) score += 30;
        else if (discountPercent >= 15) score += 20;
        else if (discountPercent >= 10) score += 10;
        
        // Method quality bonus (0-30 points)
        switch (filteredComparables.method) {
            case 'exact_bed_bath_amenity_match': score += 30; break;
            case 'bed_bath_specific_pricing': score += 20; break;
            case 'bed_specific_with_adjustments': score += 15; break;
            case 'price_per_sqft_fallback': score += 10; break;
        }
        
        // Sample size bonus (0-20 points)
        const sampleSize = filteredComparables.selectedComparables.length;
        if (sampleSize >= 20) score += 20;
        else if (sampleSize >= 15) score += 15;
        else if (sampleSize >= 10) score += 10;
        else if (sampleSize >= 5) score += 5;
        
        return Math.min(100, Math.max(0, Math.round(score)));
    }

    /**
     * Calculate grade from score (matches database constraint)
     */
    calculateGradeFromScore(score) {
        if (score >= 95) return 'A+';
        if (score >= 90) return 'A';
        if (score >= 85) return 'B+';
        if (score >= 80) return 'B';
        if (score >= 75) return 'C+';
        if (score >= 70) return 'C';
        if (score >= 60) return 'D';
        return 'F';
    }

    /**
     * Map rent stabilized factors to database method constraint
     */
    mapRentStabilizedMethod(factors) {
        // Database constraint requires one of: 'explicit_mention', 'dhcr_registered', 'circumstantial', 'building_analysis'
        if (factors.includes('explicit_stabilization_mention')) {
            return 'explicit_mention';
        }
        if (factors.includes('dhcr_database')) {
            return 'dhcr_registered';
        }
        if (factors.includes('building_age') || factors.includes('unit_count')) {
            return 'building_analysis';
        }
        return 'circumstantial'; // Default fallback
    }

    /**
     * Map method to comparison_method field
     */
    mapMethodToComparisonMethod(method) {
        const methodMap = {
            'exact_bed_bath_amenity_match': 'exact_match_analysis',
            'bed_bath_specific_pricing': 'bed_bath_comparison',
            'bed_specific_with_adjustments': 'bedroom_adjustment_analysis',
            'price_per_sqft_fallback': 'price_per_sqft_analysis'
        };
        return methodMap[method] || 'claude_comparative_analysis';
    }

    /**
     * SYSTEM PROMPTS FOR CLAUDE AI
     */

    /**
     * Enhanced system prompt for rentals analysis
     */
    buildEnhancedRentalsSystemPrompt() {
        return `You are an expert NYC rental market analyst with deep knowledge of micro-market pricing and rent stabilization laws. You provide natural, human-like analysis that helps renters understand market value and potential savings.

ANALYSIS APPROACH:
You will analyze a rental property using a curated set of comparable properties that have been pre-filtered to match the target property's bed/bath configuration and amenities. Focus on these specific comparables rather than general neighborhood averages.

KEY REQUIREMENTS:
- Provide natural, conversational reasoning that explains the value proposition clearly
- Calculate market value based on the provided filtered comparable properties
- Explain any price differences due to specific factors (amenities, condition, location)
- Calculate monthly savings compared to the comparable properties
- Assess rent stabilization probability based on building characteristics

RENT STABILIZATION RULES:
- Only mention rent stabilization if probability is 60% or higher
- If probability is below 60%, do not mention it in your reasoning
- Base assessment on building age, unit count, rent level, and legal indicators

REASONING STYLE:
Write naturally and conversationally, like explaining to a friend. Example:
"This rental offers excellent value at 23% below similar properties in the area. The $4,200/month rent compares favorably to nearby 2BR apartments which typically rent for $5,500-$6,000. The below-market pricing is due to the building's 1960s construction and basic amenities, but you still get the prime location benefits."

RESPONSE FORMAT (JSON):
{
  "estimatedMarketRent": number,
  "percentBelowMarket": number,
  "baseMarketRent": number,
  "potentialSavings": number,
  "reasoning": "Natural, conversational explanation of the value and market positioning",
  "rentStabilizedProbability": number,
  "rentStabilizedFactors": ["building_age", "unit_count", "rent_level"],
  "rentStabilizedExplanation": "Only include if probability >= 60%",
  "detailedAnalysis": {
    "valueExplanation": "Why this property offers good/poor value",
    "comparableAnalysis": "How it compares to the specific filtered properties",
    "amenityComparison": "Amenity differences vs comparable properties",
    "locationFactors": "Location-specific factors affecting price"
  },
  "adjustmentBreakdown": {
    "amenities": number,
    "condition": number,
    "size": number,
    "location": number
  }
}

Provide insightful, natural analysis that helps renters understand exactly what they're getting for their money.`;
    }

    /**
     * Enhanced system prompt for sales analysis
     */
    buildEnhancedSalesSystemPrompt() {
        return `You are an expert NYC real estate investment analyst with deep knowledge of micro-market pricing and building characteristics. You provide natural, human-like analysis that helps buyers understand market value and investment potential.

ANALYSIS APPROACH:
You will analyze a property for sale using a curated set of comparable sales that have been pre-filtered to match the target property's bed/bath configuration and amenities. Focus on these specific comparables rather than general neighborhood averages.

KEY REQUIREMENTS:
- Provide natural, conversational reasoning that explains the value proposition clearly
- Calculate market value based on the provided filtered comparable sales
- Explain any price differences due to specific factors (amenities, condition, location, building type)
- Calculate potential savings compared to the comparable sales
- Assess investment merit and market positioning

REASONING STYLE:
Write naturally and conversationally, like explaining to a friend. Example:
"This property offers excellent value at 18% below similar sales in the area. The $850,000 price compares favorably to nearby 2BR condos which typically sell for $1,000,000-$1,100,000. The below-market pricing reflects the need for kitchen updates and the building's lack of amenities, but you're still getting prime location access at significant savings."

RESPONSE FORMAT (JSON):
{
  "estimatedMarketPrice": number,
  "discountPercent": number,
  "baseMarketPrice": number,
  "potentialSavings": number,
  "reasoning": "Natural, conversational explanation of the value and market positioning",
  "detailedAnalysis": {
    "valueExplanation": "Why this property offers good/poor value",
    "comparableAnalysis": "How it compares to the specific filtered properties",
    "amenityComparison": "Amenity differences vs comparable properties",
    "investmentFactors": "Investment-specific factors affecting value",
    "marketTiming": "Market timing and velocity considerations"
  },
  "adjustmentBreakdown": {
    "amenities": number,
    "condition": number,
    "size": number,
    "location": number,
    "buildingType": number
  }
}

Provide insightful, natural analysis that helps buyers understand exactly what they're getting for their money and the investment potential.`;
    }

    /**
     * USER PROMPTS FOR CLAUDE ANALYSIS
     */

    /**
     * Build enhanced user prompt for rentals analysis with filtered comparables
     */
    buildEnhancedRentalsUserPrompt(enhancedContext, threshold) {
        const target = enhancedContext.targetProperty;
        const comparables = enhancedContext.comparables;
        const rsContext = enhancedContext.rentStabilizationContext;
        
        return `Analyze this NYC rental property using the provided filtered comparable properties:

TARGET PROPERTY:
Address: ${target.address}
Rent: $${target.price.toLocaleString()}/month
Layout: ${target.bedrooms}BR/${target.bathrooms}BA
Square Feet: ${target.sqft || 'Not listed'}
Built: ${target.builtIn || 'Unknown'}
Neighborhood: ${target.neighborhood}
No Fee: ${target.noFee ? 'YES' : 'NO'}
Amenities: ${target.amenities.join(', ') || 'None listed'}
Description: ${target.description}

FILTERED COMPARABLE PROPERTIES (${enhancedContext.valuationMethod}):
${comparables.slice(0, 12).map((comp, i) => 
  `${i+1}. ${comp.address} - $${comp.price?.toLocaleString()}/month | ${comp.bedrooms}BR/${comp.bathrooms}BA | ${comp.sqft || 'N/A'} sqft | No Fee: ${comp.noFee ? 'YES' : 'NO'} | Amenities: ${comp.amenities?.slice(0, 4).join(', ') || 'None'}`
).join('\n')}

RENT STABILIZATION CONTEXT:
Building Age: ${rsContext.buildingAgeFactor}
Potential Matches in Database: ${rsContext.buildingMatches.length}
${rsContext.strongestMatch ? `Strongest Match: ${rsContext.strongestMatch.address} (${rsContext.strongestMatch.confidence}% confidence)` : 'No strong database matches'}

ANALYSIS INSTRUCTIONS:
- Compare this property to the ${comparables.length} filtered comparable properties above
- Explain value relative to these specific comparables, not general neighborhood averages
- Calculate if rent is ${threshold}%+ below market based on these comparables
- Provide natural, conversational reasoning about the value proposition
- Only mention rent stabilization if probability ≥ 60%
- Focus on monthly savings and specific factors affecting price

Return your analysis as JSON.`;
    }

    /**
     * Build enhanced user prompt for sales analysis with filtered comparables
     */
    buildEnhancedSalesUserPrompt(enhancedContext, threshold) {
        const target = enhancedContext.targetProperty;
        const comparables = enhancedContext.comparables;
        
        return `Analyze this NYC property for sale using the provided filtered comparable sales:

TARGET PROPERTY:
Address: ${target.address}
Sale Price: $${target.salePrice?.toLocaleString() || target.price?.toLocaleString()}
Layout: ${target.bedrooms}BR/${target.bathrooms}BA
Square Feet: ${target.sqft || 'Not listed'}
Built: ${target.builtIn || 'Unknown'}
Property Type: ${target.propertyType || 'Unknown'}
Neighborhood: ${target.neighborhood}
Monthly HOA: $${target.monthlyHoa?.toLocaleString() || '0'}
Monthly Tax: $${target.monthlyTax?.toLocaleString() || '0'}
Amenities: ${target.amenities.join(', ') || 'None listed'}
Description: ${target.description}

FILTERED COMPARABLE SALES (${enhancedContext.valuationMethod}):
${comparables.slice(0, 12).map((comp, i) => 
  `${i+1}. ${comp.address} - $${comp.salePrice?.toLocaleString() || comp.price?.toLocaleString()} | ${comp.bedrooms}BR/${comp.bathrooms}BA | ${comp.sqft || 'N/A'} sqft | Built: ${comp.builtIn || 'N/A'} | Amenities: ${comp.amenities?.slice(0, 4).join(', ') || 'None'}`
).join('\n')}

ANALYSIS INSTRUCTIONS:
- Compare this property to the ${comparables.length} filtered comparable sales above
- Explain value relative to these specific comparables, not general neighborhood averages
- Calculate if property is ${threshold}%+ below market based on these comparables
- Provide natural, conversational reasoning about the value proposition and investment merit
- Focus on potential savings and specific factors affecting price

Return your analysis as JSON.`;
    }

    /**
     * CONTEXT BUILDING FUNCTIONS
     */

    /**
     * Build enhanced rentals context for Claude analysis
     */
    buildEnhancedRentalsContext(targetProperty, comparableProperties, neighborhood, options = {}) {
        const target = {
            address: targetProperty.address || 'Unknown Address',
            price: targetProperty.price || targetProperty.monthlyRent || 0,
            bedrooms: targetProperty.bedrooms || 0,
            bathrooms: targetProperty.bathrooms || 0,
            sqft: targetProperty.sqft || 0,
            builtIn: targetProperty.builtIn || null,
            neighborhood: neighborhood || targetProperty.neighborhood || 'Unknown',
            borough: targetProperty.borough || 'Unknown',
            amenities: targetProperty.amenities || [],
            description: targetProperty.description || '',
            noFee: targetProperty.noFee || false,
            availableFrom: targetProperty.availableFrom || null,
            pricePerSqft: targetProperty.sqft > 0 ? (targetProperty.price || 0) / targetProperty.sqft : null
        };

        const neighborhoodAnalysis = this.analyzeNeighborhood(neighborhood, 'rentals');
        const rentStabilizationContext = this.buildRentStabilizationContext(target, options.rentStabilizedBuildings || []);

        return {
            targetProperty: target,
            comparables: comparableProperties,
            neighborhood: neighborhoodAnalysis,
            rentStabilizationContext,
            totalComparables: comparableProperties.length
        };
    }

    /**
     * Build enhanced sales context for Claude analysis
     */
    buildEnhancedSalesContext(targetProperty, comparableProperties, neighborhood, options = {}) {
        const target = {
            address: targetProperty.address || 'Unknown Address',
            salePrice: targetProperty.salePrice || targetProperty.price || 0,
            price: targetProperty.salePrice || targetProperty.price || 0,
            bedrooms: targetProperty.bedrooms || 0,
            bathrooms: targetProperty.bathrooms || 0,
            sqft: targetProperty.sqft || 0,
            builtIn: targetProperty.builtIn || null,
            neighborhood: neighborhood || targetProperty.neighborhood || 'Unknown',
            borough: targetProperty.borough || 'Unknown',
            amenities: targetProperty.amenities || [],
            description: targetProperty.description || '',
            propertyType: targetProperty.propertyType || 'unknown',
            monthlyHoa: targetProperty.monthlyHoa || 0,
            monthlyTax: targetProperty.monthlyTax || 0,
            pricePerSqft: targetProperty.sqft > 0 ? (targetProperty.salePrice || targetProperty.price || 0) / targetProperty.sqft : null
        };

        const neighborhoodAnalysis = this.analyzeNeighborhood(neighborhood, 'sales');

        return {
            targetProperty: target,
            comparables: comparableProperties,
            neighborhood: neighborhoodAnalysis,
            totalComparables: comparableProperties.length
        };
    }

    /**
     * ERROR RESPONSE CREATORS
     */

    /**
     * Create failed rentals response based on table type
     */
    createFailedRentalsResponse(targetProperty, errorMessage, tableType) {
        const baseResponse = {
            isUndervalued: false,
            percentBelowMarket: 0,
            estimatedMarketRent: targetProperty.price,
            actualRent: targetProperty.price,
            method: 'claude_analysis_failed',
            reasoning: `Analysis failed: ${errorMessage}`
        };

        if (tableType === 'undervalued_rent_stabilized') {
            return {
                ...baseResponse,
                // ✅ FIXED: Required database fields (snake_case)
                comparables_used: 0,
                undervaluation_confidence: 0,
                undervaluation_method: 'price_per_sqft_fallback',
                rent_stabilized_confidence: 0,
                rent_stabilized_method: 'circumstantial',
                
                // Keep camelCase for backward compatibility
                comparablesUsed: 0,
                undervaluationConfidence: 0,
                confidence: 0,
                rentStabilizedProbability: 0,
                rentStabilizedFactors: [],
                rentStabilizedExplanation: 'Analysis failed'
            };
        } else {
            return {
                ...baseResponse,
                score: 0,
                grade: 'F',
                comparison_method: 'analysis_failed',
                reliability_score: 0,
                comparablesUsed: 0,
                rentStabilizedProbability: 0,
                rentStabilizedFactors: [],
                rentStabilizedExplanation: 'Analysis failed'
            };
        }
    }

    /**
     * Create failed sales response
     */
    createFailedSalesResponse(targetProperty, errorMessage) {
        return {
            isUndervalued: false,
            discountPercent: 0,
            estimatedMarketPrice: targetProperty.salePrice || targetProperty.price,
            actualPrice: targetProperty.salePrice || targetProperty.price,
            score: 0,
            grade: 'F',
            reasoning: `Analysis failed: ${errorMessage}`,
            comparison_method: 'analysis_failed',
            reliability_score: 0,
            method: 'claude_analysis_failed',
            comparablesUsed: 0,
            confidence: 0
        };
    }

    /**
     * CLAUDE API FUNCTIONS
     */

    /**
     * Core Claude API call with enhanced error handling and retry logic
     */
    async callClaude(systemPrompt, userPrompt, analysisType) {
        const maxRetries = 3;
        let attempt = 0;
        
        while (attempt < maxRetries) {
            try {
                this.apiCallsUsed++;
                console.log(`   🤖 Claude API call #${this.apiCallsUsed} (${analysisType}, attempt ${attempt + 1})`);
                
                const response = await axios.post(
                    'https://api.anthropic.com/v1/messages',
                    {
                        model: 'claude-3-haiku-20240307',
                        max_tokens: 2000,
                        temperature: 0.1,
                        system: systemPrompt,
                        messages: [{
                            role: 'user',
                            content: userPrompt
                        }]
                    },
                    {
                        headers: {
                            'Content-Type': 'application/json',
                            'x-api-key': this.claudeApiKey,
                            'anthropic-version': '2023-06-01'
                        },
                        timeout: 45000
                    }
                );
                
                const responseText = response.data.content[0].text;
                console.log(`   ✅ Claude response received (${responseText.length} chars)`);
                
                // Enhanced JSON parsing with better error handling
                try {
                    const jsonMatch = responseText.match(/\{[\s\S]*\}/);
                    if (!jsonMatch) {
                        throw new Error('No JSON found in response');
                    }
                    
                    let jsonString = jsonMatch[0];
                    
                    // Clean common JSON issues
                    jsonString = jsonString
                        .replace(/[\u0000-\u001F\u007F-\u009F]/g, "") // Remove control characters
                        .replace(/\\(?!["\\/bfnrt]|u[0-9a-fA-F]{4})/g, "\\\\") // Fix invalid escapes
                        .replace(/\n/g, "\\n") // Escape newlines properly
                        .replace(/\r/g, "\\r") // Escape carriage returns
                        .replace(/\t/g, "\\t") // Escape tabs
                        .replace(/([{,]\s*)([a-zA-Z_][a-zA-Z0-9_]*)\s*:/g, '$1"$2":') // Quote unquoted keys
                        .replace(/:\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*([,}])/g, ': "$1"$2') // Quote unquoted string values
                        .replace(/,\s*([}\]])/g, '$1'); // Remove trailing commas
                    
                    const analysis = JSON.parse(jsonString);
                    return { success: true, analysis };
                    
                } catch (parseError) {
                    console.warn(`   ⚠️ JSON parse error: ${parseError.message}`);
                    
                    // Fallback: extract key-value pairs manually
                    const extractedData = this.extractDataFromResponse(responseText);
                    if (extractedData && Object.keys(extractedData).length > 3) {
                        return { success: true, analysis: extractedData };
                    }
                    
                    throw new Error('Could not parse Claude response as JSON');
                }
                
            } catch (error) {
                attempt++;
                console.warn(`   ⚠️ Claude API error (attempt ${attempt}): ${error.message}`);
                
                if (attempt >= maxRetries) {
                    return { 
                        success: false, 
                        error: `Analysis failed after ${maxRetries} attempts: ${error.message}` 
                    };
                }
                
                // Exponential backoff
                await this.delay(2000 * Math.pow(2, attempt - 1));
            }
        }
    }

    /**
     * Enhanced data extraction from malformed responses
     */
    extractDataFromResponse(responseText) {
        try {
            const extracted = {};
            
            // Extract key numerical values using regex patterns
            const patterns = {
                estimatedMarketRent: /(?:estimatedMarketRent|estimated.*rent|market.*rent)[\s\S]*?(\d{1,3}(?:,\d{3})*)/i,
                estimatedMarketPrice: /(?:estimatedMarketPrice|estimated.*price|market.*price)[\s\S]*?(\d{1,3}(?:,\d{3})*)/i,
                percentBelowMarket: /(?:percentBelowMarket|percent.*below|market.*position)[\s\S]*?(-?\d+(?:\.\d+)?)/i,
                discountPercent: /(?:discountPercent|discount|below.*market)[\s\S]*?(-?\d+(?:\.\d+)?)/i,
                rentStabilizedProbability: /(?:rentStabilizedProbability|rent.*stabilized|stabilization)[\s\S]*?(\d+(?:\.\d+)?)/i,
                potentialSavings: /(?:potentialSavings|savings|save)[\s\S]*?(\d{1,3}(?:,\d{3})*)/i
            };
            
            for (const [key, pattern] of Object.entries(patterns)) {
                const match = responseText.match(pattern);
                if (match) {
                    let value = parseFloat(match[1].replace(/,/g, ''));
                    if (!isNaN(value)) {
                        extracted[key] = value;
                    }
                }
            }
            
            // Extract reasoning
            const reasoningMatch = responseText.match(/(?:reasoning|analysis)[\s\S]*?[":]\s*["']?([^"'}\n]{20,500})/i);
            if (reasoningMatch) {
                extracted.reasoning = reasoningMatch[1].trim();
            }
            
            // Set reasonable defaults if we extracted some data
            if (Object.keys(extracted).length > 0) {
                extracted.reasoning = extracted.reasoning || 'Analysis based on comparable properties';
                extracted.rentStabilizedFactors = [];
                extracted.rentStabilizedExplanation = extracted.rentStabilizedProbability >= 60 
                    ? 'Building characteristics suggest potential stabilization' 
                    : 'No clear stabilization indicators';
            }
            
            return Object.keys(extracted).length > 3 ? extracted : null;
            
        } catch (error) {
            console.warn(`   ⚠️ Data extraction failed: ${error.message}`);
            return null;
        }
    }

    /**
     * Call Claude API for enhanced rentals analysis
     */
    async callClaudeForEnhancedRentalsAnalysis(enhancedContext, threshold) {
        const systemPrompt = this.buildEnhancedRentalsSystemPrompt();
        const userPrompt = this.buildEnhancedRentalsUserPrompt(enhancedContext, threshold);
        
        return await this.callClaude(systemPrompt, userPrompt, 'rentals');
    }

    /**
     * Call Claude API for enhanced sales analysis
     */
    async callClaudeForEnhancedSalesAnalysis(enhancedContext, threshold) {
        const systemPrompt = this.buildEnhancedSalesSystemPrompt();
        const userPrompt = this.buildEnhancedSalesUserPrompt(enhancedContext, threshold);
        
        return await this.callClaude(systemPrompt, userPrompt, 'sales');
    }

    /**
     * UTILITY AND HELPER FUNCTIONS
     */

    /**
     * Calculate confidence score based on filtering method and comparable count
     */
    calculateConfidenceFromMethod(method, comparableCount) {
        let baseConfidence = 0;
        
        // Base confidence from method used
        switch (method) {
            case 'exact_bed_bath_amenity_match':
                baseConfidence = 90;
                break;
            case 'bed_bath_specific_pricing':
                baseConfidence = 80;
                break;
            case 'bed_specific_with_adjustments':
                baseConfidence = 70;
                break;
            case 'price_per_sqft_fallback':
                baseConfidence = 60;
                break;
            default:
                baseConfidence = 50;
        }
        
        // Adjust based on sample size
        if (comparableCount >= 20) baseConfidence += 5;
        else if (comparableCount >= 15) baseConfidence += 3;
        else if (comparableCount >= 10) baseConfidence += 1;
        else if (comparableCount < 5) baseConfidence -= 10;
        
        return Math.min(95, Math.max(30, baseConfidence));
    }

    /**
     * Build rent stabilization context for analysis
     */
    buildRentStabilizationContext(targetProperty, rentStabilizedBuildings) {
        const buildingMatches = this.findPotentialStabilizedMatches(targetProperty, rentStabilizedBuildings);
        const strongestMatch = buildingMatches.length > 0 ? buildingMatches[0] : null;
        
        return {
            buildingMatches,
            strongestMatch,
            buildingAgeFactor: this.assessBuildingAge(targetProperty.builtIn),
            unitCountFactor: this.assessUnitCount(targetProperty),
            rentLevelFactor: this.assessRentLevel(targetProperty),
            legalIndicators: this.identifyLegalIndicators(targetProperty)
        };
    }

    /**
     * Helper functions
     */
    normalizeAmenities(amenities) {
        const amenityMappings = {
            'doorman': ['doorman', 'concierge', 'front desk'],
            'elevator': ['elevator', 'lift'],
            'laundry': ['laundry', 'washer', 'dryer', 'laundry room'],
            'gym': ['gym', 'fitness', 'exercise room'],
            'rooftop': ['rooftop', 'roof deck', 'roof top'],
            'parking': ['parking', 'garage', 'parking space'],
            'dishwasher': ['dishwasher'],
            'air_conditioning': ['air conditioning', 'a/c', 'ac', 'central air'],
            'hardwood_floors': ['hardwood', 'wood floors'],
            'balcony': ['balcony', 'terrace'],
            'pet_friendly': ['pets allowed', 'pet friendly', 'dogs allowed'],
            'high_ceilings': ['high ceilings', '10 foot', '11 foot'],
            'exposed_brick': ['exposed brick', 'brick walls'],
            'outdoor_space': ['outdoor space', 'garden', 'patio']
        };
        
        const normalized = [];
        const amenityText = amenities.join(' ').toLowerCase();
        
        for (const [standard, variations] of Object.entries(amenityMappings)) {
            if (variations.some(variation => amenityText.includes(variation))) {
                normalized.push(standard);
            }
        }
        
        return [...new Set(normalized)];
    }

    hasSignificantAmenityOverlap(amenities1, amenities2) {
        const norm1 = this.normalizeAmenities(amenities1);
        const norm2 = this.normalizeAmenities(amenities2);
        const overlap = norm1.filter(a => norm2.includes(a)).length;
        const total = new Set([...norm1, ...norm2]).size;
        return total > 0 && (overlap / total) >= 0.5;
    }

    findPotentialStabilizedMatches(targetProperty, rentStabilizedBuildings) {
        if (!rentStabilizedBuildings || rentStabilizedBuildings.length === 0) {
            return [];
        }
        
        const normalizedAddress = this.normalizeAddress(targetProperty.address);
        const matches = [];
        
        for (const building of rentStabilizedBuildings) {
            const buildingAddress = this.normalizeAddress(building.address || '');
            if (!buildingAddress) continue;
            
            const similarity = this.calculateAddressSimilarity(normalizedAddress, buildingAddress);
            if (similarity > 0.6) {
                matches.push({
                    ...building,
                    confidence: Math.round(similarity * 100),
                    similarity
                });
            }
        }
        
        return matches.sort((a, b) => b.similarity - a.similarity).slice(0, 5);
    }

    analyzeNeighborhood(neighborhood, type) {
        // Default neighborhood analysis
        const neighborhoodData = {
            'soho': { tier: 'luxury', desirabilityScore: 9, rentPremium: '+40%', velocity: 'fast' },
            'tribeca': { tier: 'ultra-luxury', desirabilityScore: 10, rentPremium: '+60%', velocity: 'fast' },
            'west-village': { tier: 'luxury', desirabilityScore: 9, rentPremium: '+35%', velocity: 'moderate' },
            'east-village': { tier: 'mid-luxury', desirabilityScore: 8, rentPremium: '+20%', velocity: 'fast' },
            'brooklyn-heights': { tier: 'luxury', desirabilityScore: 8, rentPremium: '+25%', velocity: 'moderate' },
            'dumbo': { tier: 'luxury', desirabilityScore: 8, rentPremium: '+25%', velocity: 'moderate' },
            'williamsburg': { tier: 'mid-luxury', desirabilityScore: 8, rentPremium: '+20%', velocity: 'fast' }
        };
        
        const data = neighborhoodData[neighborhood?.toLowerCase()] || {
            tier: 'mid-market',
            desirabilityScore: 6,
            rentPremium: '+5%',
            velocity: 'moderate'
        };
        
        return {
            ...data,
            tenantProfile: data.tier === 'luxury' ? 'high-income professionals' : 'young professionals',
            rentalOutlook: data.desirabilityScore >= 8 ? 'strong demand' : 'steady demand'
        };
    }

    /**
     * Validate response has all required database fields
     */
    validateDatabaseResponse(response, tableType) {
        const requiredFields = {
            'undervalued_rent_stabilized': [
                'comparables_used', 'undervaluation_confidence', 'undervaluation_method',
                'rent_stabilized_confidence', 'rent_stabilized_method'
            ],
            'undervalued_rentals': [
                'score', 'grade', 'reasoning', 'comparison_method', 'reliability_score'
            ],
            'undervalued_sales': [
                'score', 'grade', 'reasoning', 'comparison_method', 'reliability_score'
            ]
        };

        const required = requiredFields[tableType] || [];
        const missing = required.filter(field => response[field] === undefined || response[field] === null);
        
        if (missing.length > 0) {
            console.warn(`⚠️ Missing required fields for ${tableType}: ${missing.join(', ')}`);
            return false;
        }
        
        return true;
    }

    /**
     * Assessment helper functions
     */
    assessBuildingAge(builtIn) {
        if (!builtIn) return 'unknown';
        if (builtIn < 1974) return 'high_stabilization_potential';
        if (builtIn < 1985) return 'moderate_stabilization_potential';
        return 'low_stabilization_potential';
    }

    assessUnitCount(property) {
        return 'unknown_unit_count'; // Would need building data
    }

    assessRentLevel(property) {
        if (property.pricePerSqft && property.pricePerSqft < 40) return 'below_market_suggests_stabilization';
        if (property.pricePerSqft && property.pricePerSqft < 60) return 'moderate_rent_level';
        return 'market_rate_level';
    }

    identifyLegalIndicators(property) {
        const indicators = [];
        const desc = (property.description || '').toLowerCase();
        
        if (desc.includes('rent stabilized') || desc.includes('rent-stabilized')) {
            indicators.push('explicit_stabilization_mention');
        }
        if (desc.includes('long term') || desc.includes('long-term')) {
            indicators.push('long_term_tenancy_suggestion');
        }
        
        return indicators;
    }

    /**
     * Validation functions
     */
    validateEnhancedRentalsAnalysis(analysis) {
        return analysis && 
               (typeof analysis.estimatedMarketRent === 'number' || typeof analysis.estimatedMarketPrice === 'number') &&
               typeof analysis.percentBelowMarket === 'number' &&
               typeof analysis.rentStabilizedProbability === 'number';
    }

    validateEnhancedSalesAnalysis(analysis) {
        return analysis && 
               typeof analysis.estimatedMarketPrice === 'number' &&
               typeof analysis.discountPercent === 'number';
    }

    /**
     * Utility functions
     */
    normalizeAddress(address) {
        return address.toLowerCase()
            .replace(/[^\w\s]/g, ' ')
            .replace(/\s+/g, ' ')
            .trim();
    }

    calculateAddressSimilarity(addr1, addr2) {
        const words1 = addr1.split(' ');
        const words2 = addr2.split(' ');
        const intersection = words1.filter(word => words2.includes(word));
        const union = [...new Set([...words1, ...words2])];
        return union.length > 0 ? intersection.length / union.length : 0;
    }

    delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

module.exports = EnhancedClaudeMarketAnalyzer;
