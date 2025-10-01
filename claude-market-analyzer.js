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
 * ENHANCED RENTALS ANALYSIS - OPTIMIZED VERSION
 * FIXED: Skip detailed analysis for overvalued properties to save processing time
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
        
        // ✅ FIXED: Proper validation of discount percentage
        const actualRent = targetProperty.price;
        const marketRent = analysis.estimatedMarketRent;
        const correctDiscount = ((marketRent - actualRent) / marketRent * 100);
        
        // ✅ DETECT OVERPRICED PROPERTIES
        if (correctDiscount < 0) {
            const overvaluedPercent = Math.abs(correctDiscount);
            console.log(`   ⚠️ OVERPRICED: ${overvaluedPercent.toFixed(1)}% above market ($${actualRent.toLocaleString()} vs $${marketRent.toLocaleString()}) - skipping detailed analysis`);
            
            return {
                isUndervalued: false,
                percentBelowMarket: correctDiscount, // Negative value indicates overpriced
                estimatedMarketRent: marketRent,
                actualRent: actualRent,
                potentialSavings: marketRent - actualRent, // Negative = overpaying
                confidence: calculatedConfidence,
                method: 'claude_hierarchical_analysis',
                comparablesUsed: filteredComparables.selectedComparables.length,
                reasoning: `This property is overpriced at $${actualRent.toLocaleString()}/month compared to market rate of $${marketRent.toLocaleString()}/month (${overvaluedPercent.toFixed(1)}% above market).`,
                undervaluationConfidence: calculatedConfidence,
                rentStabilizedProbability: analysis.rentStabilizedProbability || 0,
                rentStabilizedFactors: analysis.rentStabilizedFactors || [],
                rentStabilizedExplanation: 'No analysis needed for overpriced property'
            };
        }
        
        // ✅ OPTIMIZATION: Check if property meets undervaluation threshold BEFORE detailed analysis
        const isUndervalued = correctDiscount >= (threshold - 0.1) && calculatedConfidence >= 60;
        
        if (!isUndervalued) {
            // Property doesn't meet threshold - return basic response without detailed analysis
            console.log(`   ⚠️ Not undervalued (${correctDiscount.toFixed(1)}% < ${threshold}%) - skipping detailed analysis`);
            
            return {
                isUndervalued: false,
                percentBelowMarket: correctDiscount,
                estimatedMarketRent: marketRent,
                actualRent: actualRent,
                potentialSavings: marketRent - actualRent,
                confidence: calculatedConfidence,
                method: 'claude_hierarchical_analysis',
                comparablesUsed: filteredComparables.selectedComparables.length,
                reasoning: `Property priced at market rate. Rent of $${actualRent.toLocaleString()}/month is only ${correctDiscount.toFixed(1)}% below estimated market rent of $${marketRent.toLocaleString()}/month.`,
                undervaluationConfidence: calculatedConfidence,
                rentStabilizedProbability: analysis.rentStabilizedProbability || 0,
                rentStabilizedFactors: analysis.rentStabilizedFactors || [],
                rentStabilizedExplanation: 'No detailed analysis for market-rate property'
            };
        }
        
        // ✅ ONLY generate detailed analysis for TRULY UNDERVALUED properties
        console.log(`   ✅ Undervalued property detected (${correctDiscount.toFixed(1)}% below market) - generating detailed analysis`);
        if (analysis.rentStabilizedProbability >= 60) {
            console.log(`   🔒 Rent stabilized: ${analysis.rentStabilizedProbability}%`);
        }
        
        // STEP 6: Generate enhanced analysis using the new functions (ONLY for undervalued)
        const rentStabilizedBuildings = options.rentStabilizedBuildings || [];
        const enhancedRentStabilization = this.generateRentStabilizationAnalysis(targetProperty, rentStabilizedBuildings);
        const enhancedUndervaluation = this.generateUndervaluationAnalysis(targetProperty, filteredComparables.selectedComparables, analysis);

        // STEP 7: Return ENHANCED structure with comprehensive analysis (ONLY for undervalued)
        return {
            isUndervalued: true,
            percentBelowMarket: correctDiscount, // Use corrected calculation
            estimatedMarketRent: marketRent,
            actualRent: actualRent,
            potentialSavings: marketRent - actualRent, // Positive for undervalued
            confidence: calculatedConfidence,
            method: 'claude_hierarchical_analysis',
            comparablesUsed: filteredComparables.selectedComparables.length,
            reasoning: analysis.reasoning || 'Claude AI market analysis',
            undervaluationConfidence: calculatedConfidence,
            
            // Enhanced rent stabilization analysis
            rentStabilizedProbability: enhancedRentStabilization.confidence_percentage || 0,
            rentStabilizedFactors: enhancedRentStabilization.key_factors || [],
            rentStabilizedExplanation: enhancedRentStabilization.detailed_explanation || 'No stabilization indicators found',
            rentStabilizedMethod: enhancedRentStabilization.analysis_method || 'building_characteristics_analysis',
            
            // Enhanced undervaluation analysis
            detailedAnalysis: enhancedUndervaluation || {},
            valuationMethod: filteredComparables.method,
            baseMarketRent: analysis.baseMarketRent || marketRent,
            adjustmentBreakdown: enhancedUndervaluation.adjustment_breakdown || {},
            legalProtectionValue: enhancedRentStabilization.confidence_percentage >= 60 ? (marketRent - actualRent) * 12 : 0,
            investmentMerit: correctDiscount >= 25 ? 'strong' : correctDiscount >= 15 ? 'moderate' : 'low',
            
            // Full enhanced data for database integration
            enhancedRentStabilization,
            enhancedUndervaluation
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
 * ENHANCED SALES ANALYSIS - FULLY FIXED VERSION
 * ✅ Corrects above/below market confusion
 * ✅ Uses validated calculations in all return values
 * ✅ Ready for deployment with no syntax errors
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

        // ✅ STEP 5: VALIDATE CLAUDE'S CALCULATION - THIS IS THE FIX
        const actualPrice = targetProperty.salePrice || targetProperty.price;
        const marketPrice = analysis.estimatedMarketPrice;
        const correctDiscount = ((marketPrice - actualPrice) / marketPrice * 100);

        // ✅ DETECT OVERPRICED SALES PROPERTIES
        if (correctDiscount < 0) {
            const overvaluedPercent = Math.abs(correctDiscount);
            console.log(`   ⚠️ OVERPRICED: ${overvaluedPercent.toFixed(1)}% above market ($${actualPrice.toLocaleString()} vs $${marketPrice.toLocaleString()}) - skipping detailed analysis`);
            
            return {
                isUndervalued: false,
                discountPercent: correctDiscount, // Negative value
                estimatedMarketPrice: marketPrice,
                actualPrice: actualPrice,
                potentialSavings: marketPrice - actualPrice, // Negative = overpaying
                confidence: 0,
                method: 'claude_analysis_overpriced',
                reasoning: `Property is overpriced by ${overvaluedPercent.toFixed(1)}% above market value`,
                comparablesUsed: filteredComparables.selectedComparables.length
            };
        }
        
        // STEP 6: Calculate confidence from method (since Claude doesn't provide it)
        const calculatedConfidence = this.calculateConfidenceFromMethod(filteredComparables.method, filteredComparables.selectedComparables.length);
        
        console.log(`   💰 Claude estimate: $${marketPrice?.toLocaleString()}`);
        console.log(`   📊 Below market: ${correctDiscount?.toFixed(1)}%`);
        
        // ✅ STEP 7: Return CORRECTED structure using validated calculations
        return {
            isUndervalued: correctDiscount >= threshold && calculatedConfidence >= 60,
            discountPercent: correctDiscount, // ✅ Use corrected discount
            estimatedMarketPrice: marketPrice,
            actualPrice: actualPrice,
            potentialSavings: marketPrice - actualPrice, // ✅ Use corrected calculation
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
        
        // Method 1: Try exact bed/bath/amenity match (minimum 300)
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
        
        // Method 2: Same bed/bath with amenity adjustments (minimum 150)
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
        
        // Method 3: Same bedrooms with bath/amenity adjustments (minimum 120)
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
        
        // Method 2: Same bed/bath with amenity adjustments (minimum 80)
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
        
        // Method 3: Same bedrooms with bath/amenity adjustments (minimum 120)
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
        if (score >= 98) return 'A+';
        if (score >= 93) return 'A';
        if (score >= 88) return 'B+';
        if (score >= 83) return 'B';
        if (score >= 75) return 'C+';
        if (score >= 70) return 'C';
        if (score >= 60) return 'D';
        return 'F';
    }

   /**
 * Map rent stabilized factors to database method constraint
 * ✅ FIXED: Updated to match new DHCR-focused factor names
 */
mapRentStabilizedMethod(factors) {
    // Just return the primary factor directly - much simpler!
    if (factors.includes('explicit_stabilization_mention')) {
        return 'explicit_stabilization_mention';
    }
    if (factors.includes('strong_dhcr_match')) {
        return 'strong_dhcr_match';
    }
    if (factors.includes('good_dhcr_match')) {
        return 'good_dhcr_match';
    }
    // ... etc, or even just return the first factor
    return factors[0] || 'unknown_method';
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

Only assign a rentStabilizedProbability ≥ 60 if there is strong evidence. This includes:
	•	Direct mention of “rent-stabilized”, “regulated”, or “long-term tenant” or similar indicators in the listing description
	•	Matching building in DHCR database
	•	Buildings constructed between 1947 and 1973 with 6 or more units are potentially rent-stabilized, but this must be confirmed through direct evidence (listing description or DHCR match).


Be cautious with assumptions based on rent level or age alone — those are not conclusive.

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
  "rentStabilizedFactors": ["dhcr_database", "building_age", "unit_count", "rent_level"],
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
          totalComparables: comparableProperties.length,
valuationMethod: 'claude_hierarchical_analysis'
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
totalComparables: comparableProperties.length,
valuationMethod: 'claude_hierarchical_analysis'
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
                        timeout: 65000
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
    // Keep your existing 7 neighborhoods
    'soho': { tier: 'luxury', desirabilityScore: 9, rentPremium: '+40%', velocity: 'fast' },
    'tribeca': { tier: 'ultra-luxury', desirabilityScore: 10, rentPremium: '+60%', velocity: 'fast' },
    'west-village': { tier: 'luxury', desirabilityScore: 9, rentPremium: '+35%', velocity: 'moderate' },
    'east-village': { tier: 'mid-luxury', desirabilityScore: 8, rentPremium: '+20%', velocity: 'fast' },
    'brooklyn-heights': { tier: 'luxury', desirabilityScore: 8, rentPremium: '+25%', velocity: 'moderate' },
    'dumbo': { tier: 'luxury', desirabilityScore: 8, rentPremium: '+25%', velocity: 'moderate' },
    'williamsburg': { tier: 'mid-luxury', desirabilityScore: 8, rentPremium: '+20%', velocity: 'fast' },
    
    // Add only the unique/emerging ones Claude might not know well
    'roosevelt-island': { tier: 'mid-market', desirabilityScore: 7, rentPremium: '+10%', velocity: 'moderate' },
    'hudson-yards': { tier: 'luxury', desirabilityScore: 9, rentPremium: '+45%', velocity: 'fast' },
    'nomad': { tier: 'mid-luxury', desirabilityScore: 8, rentPremium: '+25%', velocity: 'fast' }
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

    delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
	
/**
 * Generate comprehensive rent stabilization analysis - FIXED VERSION
 * ✅ REQUIRES DHCR address match as prerequisite - no more inflated values!
 * ✅ ADDED: Description check for explicit mentions
 */
generateRentStabilizationAnalysis(property, rentStabilizedBuildings = []) {
    const buildingAge = this.calculateBuildingAge(property.builtIn);
    const buildingType = this.analyzeBuildingType(property);
    const unitCount = this.estimateUnitCount(property);
    const rentLevel = this.analyzeRentLevel(property);
    
    // ✅ NEW: STEP 0 - CHECK FOR EXPLICIT MENTION IN DESCRIPTION (HIGHEST PRIORITY)
    const description = (property.description || '').toLowerCase();
    
    // Quick hardcoded check for obvious mentions
    const hasObviousMention = description.includes('rent stabilized') || 
                             description.includes('rent-stabilized') || 
                             description.includes('stabilized rent') ||
                             description.includes('regulated rent') ||
                             description.includes('rent controlled');
    
    if (hasObviousMention) {
        console.log(`   🎯 OBVIOUS rent stabilization mention found in description - 100% confidence`);
        return this.createExplicitStabilizationResponse(property, buildingAge, unitCount, rentLevel, 'obvious_description_mention');
    }
    
    // ✅ CLAUDE ANALYSIS: Check for subtle mentions if no obvious ones found
    const claudeStabilizationCheck = this.analyzeDescriptionForStabilization(description);
    
    if (claudeStabilizationCheck.hasStabilizationMention && claudeStabilizationCheck.confidence >= 85) {
        console.log(`   🧠 CLAUDE detected rent stabilization mention (${claudeStabilizationCheck.confidence}% confidence): "${claudeStabilizationCheck.evidence}"`);
        return this.createExplicitStabilizationResponse(property, buildingAge, unitCount, rentLevel, 'claude_description_analysis', claudeStabilizationCheck);
    }
    
    // ✅ STEP 1: DHCR MATCH IS NOW REQUIRED - No exceptions! (UNCHANGED FROM YOUR VERSION)
    const dhcrMatches = this.findDHCRMatches(property, rentStabilizedBuildings);
    
    // ✅ MANDATORY DHCR REQUIREMENT: If no DHCR match, immediately return low confidence
    if (dhcrMatches.length === 0) {
        console.log(`   ❌ No DHCR match found for ${property.address} - not flagging as rent-stabilized`);
        return {
            confidence_percentage: 15, // Very low confidence without DHCR match
            analysis_method: 'no_dhcr_match',
            key_factors: ['Building not found in rent-stabilized database'],
            legal_factors: ['No matching address found in rent-stabilized building registry'],
            detailed_explanation: `This building was not found in the official rent stabilization registry. Without official registration, there is minimal likelihood of rent stabilization despite other building characteristics.`,
            building_age_factor: {
                construction_year: property.builtIn,
                era: buildingAge.era,
                years_old: buildingAge.age,
                is_stabilization_era: buildingAge.isRentStabilizedEra
            },
            unit_count_factor: {
                estimated_units: unitCount.estimate,
                meets_minimum: unitCount.estimate >= 6,
                confidence: unitCount.confidence
            },
            rent_level_factor: {
                level: rentLevel.level,
                variance_from_market: rentLevel.variance,
                monthly_rent: property.price || property.monthlyRent
            },
            dhcr_matches: [] // Empty since no matches found
        };
    }
    
    // ✅ STEP 2: DHCR match found - now evaluate strength + building characteristics (UNCHANGED FROM YOUR VERSION)
    let confidence = 40; // Start with moderate confidence due to DHCR match
    let factors = [];
    let legalFactors = [];
    let analysis = "";
    
    const bestMatch = dhcrMatches[0];
    console.log(`   ✅ DHCR match found: ${bestMatch.address} (${Math.round(bestMatch.similarity * 100)}% similarity)`);
    
    // DHCR DATABASE MATCH ANALYSIS (Primary Factor)
    if (bestMatch.similarity >= 0.9) {
        confidence += 35; // Strong match
        factors.push('Building matches rent-stabilized registry (strong match)');
legalFactors.push(`Strong registry match: ${bestMatch.address} (${Math.round(bestMatch.similarity * 100)}% similarity)`);
analysis += `This building is listed in the official rent stabilization registry with a strong address match (${Math.round(bestMatch.similarity * 100)}% similarity), providing legal confirmation of rent-stabilized units. `;
    } else if (bestMatch.similarity >= 0.7) {
        confidence += 25; // Good match
        factors.push('Building matches rent-stabilized registry (good match)');
legalFactors.push(`Good registry match: ${bestMatch.address} (${Math.round(bestMatch.similarity * 100)}% similarity)`);
analysis += `This building appears in the official rent stabilization registry with a good address match (${Math.round(bestMatch.similarity * 100)}% similarity), supporting rent-stabilized status. `;
    } else {
        confidence += 15; // Weak but acceptable match
        factors.push('Building may match rent-stabilized registry (possible match)');
legalFactors.push(`Possible registry match: ${bestMatch.address} (${Math.round(bestMatch.similarity * 100)}% similarity)`);
analysis += `This building may appear in the rent stabilization registry with a possible address match (${Math.round(bestMatch.similarity * 100)}% similarity), suggesting potential rent stabilization. `;
    }
    
    // BUILDING AGE ANALYSIS (Supporting Factor)
    if (buildingAge.isRentStabilizedEra) {
        if (buildingAge.era === 'prime_stabilization' && property.builtIn >= 1947 && property.builtIn <= 1973) {
            confidence += 20; // Reduced from 40 since DHCR is primary
            factors.push(`Built ${property.builtIn} (prime rent-stabilization era 1947-1973)`);
            legalFactors.push(`Built in ${property.builtIn}, within prime rent stabilization period (1947-1973)`);
            analysis += `The building's ${property.builtIn} construction date falls within the prime rent stabilization era, supporting the DHCR registration. `;
        } else if (buildingAge.era === 'emergency_rent_control' && property.builtIn >= 1943 && property.builtIn <= 1946) {
            confidence += 15;
factors.push(`Built ${property.builtIn} (emergency rent control era 1943-1946)`);
            legalFactors.push(`Built in ${property.builtIn}, during Emergency Rent Control period (1943-1946)`);
            analysis += `Built during the Emergency Rent Control period (${property.builtIn}), consistent with DHCR registration. `;
        } else if (buildingAge.era === 'early_stabilization' && property.builtIn >= 1974 && property.builtIn <= 1983) {
            confidence += 10;
factors.push(`Built ${property.builtIn} (early post-stabilization era)`);
            legalFactors.push(`Built in ${property.builtIn}, during early post-stabilization period`);
            analysis += `The ${property.builtIn} construction date is consistent with buildings that retained rent stabilization. `;
        }
    }
    
    // UNIT COUNT ANALYSIS (Supporting Factor)
    if (unitCount.estimate >= 6) {
        confidence += 10; // Reduced from 20 since DHCR is primary
        factors.push(`6+ units (legal minimum for rent stabilization)`);
        legalFactors.push(`Estimated ${unitCount.estimate}+ units, meeting 6+ unit requirement`);
        analysis += `The estimated ${unitCount.estimate} units meets the minimum threshold required for rent stabilization. `;
    }
    
    // RENT LEVEL ANALYSIS (Supporting Factor)
    if (rentLevel.level === 'below_market' && rentLevel.variance <= -15) {
        confidence += 10; // Reduced from 15 since DHCR is primary
factors.push(`Below-market rent (${Math.abs(rentLevel.variance)}% under market suggests rent controls)`);
        legalFactors.push(`Rent is ${Math.abs(rentLevel.variance)}% below market, supporting stabilization controls`);
        analysis += `The below-market rent level (${Math.abs(rentLevel.variance)}% under market) is consistent with rent stabilization controls. `;
    }
    
    // BUILDING TYPE FACTOR (Minor Factor)
    if (buildingType.category === 'traditional_rental') {
        confidence += 5; // Reduced from 10
factors.push('Traditional rental building (commonly rent-stabilized)');
        legalFactors.push('Traditional rental building type, commonly rent-stabilized');
        analysis += `The traditional rental building type is commonly subject to rent stabilization. `;
    } else if (buildingType.category === 'luxury_condo_conversion') {
        confidence -= 10;
        legalFactors.push('Luxury condo conversion may have fewer stabilized units');
        analysis += `Despite the luxury conversion, DHCR records suggest some units remain stabilized. `;
    }
    
    // Cap confidence at 95%
    confidence = Math.min(95, Math.max(15, confidence));
    
    // Determine method based on DHCR match strength
    let method = 'dhcr_verified_analysis';
    if (bestMatch.similarity >= 0.9) {
        method = 'strong_dhcr_verification';
    } else if (bestMatch.similarity >= 0.7) {
        method = 'moderate_dhcr_verification';
    } else {
        method = 'weak_dhcr_verification';
    }
    
    // Final summary
    if (confidence >= 80) {
        analysis += `With ${confidence}% confidence and DHCR verification, this unit is highly likely to be rent-stabilized.`;
    } else if (confidence >= 60) {
        analysis += `With ${confidence}% confidence and DHCR database support, this unit has good likelihood of being rent-stabilized.`;
    } else {
        analysis += `With ${confidence}% confidence, rent stabilization is possible but uncertain despite DHCR database presence.`;
    }
    
    return {
        confidence_percentage: confidence,
        analysis_method: method,
        key_factors: factors,
        legal_factors: legalFactors,
        detailed_explanation: analysis,
        building_age_factor: {
            construction_year: property.builtIn,
            era: buildingAge.era,
            years_old: buildingAge.age,
            is_stabilization_era: buildingAge.isRentStabilizedEra
        },
        unit_count_factor: {
            estimated_units: unitCount.estimate,
            meets_minimum: unitCount.estimate >= 6,
            confidence: unitCount.confidence
        },
        rent_level_factor: {
            level: rentLevel.level,
            variance_from_market: rentLevel.variance,
            monthly_rent: property.price || property.monthlyRent
        },
        dhcr_matches: dhcrMatches.map(match => ({
            address: match.address,
            similarity: Math.round(match.similarity * 100),
            status: match.status1 || 'Multiple Dwelling'
        }))
    };
}

/**
 * Generate comprehensive undervaluation analysis
 * FIXED: Corrected discount calculation logic for overpriced properties
 */
generateUndervaluationAnalysis(property, comparables, marketAnalysis) {
    const buildingType = this.analyzeBuildingType(property);
    const condition = this.analyzePropertyCondition(property);
    const amenities = this.analyzeAmenities(property);
    const location = this.analyzeLocationValue(property);
    
    let analysis = "";
    let adjustments = [];
    let confidence = marketAnalysis.confidence || 70;
    let methodology = marketAnalysis.method || 'comparative_market_analysis';
const keyAmenities = []; // ← ADD THIS LINE
    
   // Start with property overview
const beds = property.bedrooms || 0;
const baths = property.bathrooms || 0;
const neighborhood = property.neighborhood || 'this area';
const actualRent = property.price || property.monthlyRent || 0;
const marketRent = marketAnalysis.estimatedMarketRent || actualRent;

// BUILDING TYPE ANALYSIS
if (buildingType.category === 'luxury_high_rise') {
    adjustments.push({
        type: 'building_type',
        factor: 'luxury_high_rise',
        impact: '+$200-400/month premium',
        explanation: 'Luxury high-rise commands premium rents'
    });
} else if (buildingType.category === 'boutique_luxury') {
    adjustments.push({
        type: 'building_type',
        factor: 'boutique_luxury',
        impact: '+$150-300/month premium',
        explanation: 'Boutique luxury buildings offer premium living experience'
    });
} else if (buildingType.category === 'traditional_rental') {
    adjustments.push({
        type: 'building_type',
        factor: 'traditional_rental',
        impact: 'Market baseline',
        explanation: 'Traditional rental building represents market baseline'
    });
} else if (buildingType.category === 'older_walk_up') {
    adjustments.push({
        type: 'building_type',
        factor: 'older_walk_up',
        impact: '-$100-200/month discount',
        explanation: 'Walk-up buildings typically rent below elevator buildings'
    });
}

// CONDITION ANALYSIS
if (condition.level === 'pristine') {
    adjustments.push({
        type: 'condition',
        factor: 'pristine_renovation',
        impact: '+$150-250/month premium',
        explanation: 'Recently renovated units with high-end finishes command premium'
    });
} else if (condition.level === 'updated') {
    adjustments.push({
        type: 'condition',
        factor: 'updated_condition',
        impact: '+$75-150/month premium',
        explanation: 'Updated units with modern amenities rent above average'
    });
} else if (condition.level === 'original') {
    adjustments.push({
        type: 'condition',
        factor: 'original_character',
        impact: 'Market baseline to slight discount',
        explanation: 'Original condition units at market baseline or slight discount'
    });
} else if (condition.level === 'needs_work') {
    adjustments.push({
        type: 'condition',
        factor: 'needs_updating',
        impact: '-$100-200/month discount',
        explanation: 'Units needing work typically rent below market'
    });
}

// AMENITIES ANALYSIS
if (amenities.hasAmenity('doorman')) {
    keyAmenities.push('doorman building');
    adjustments.push({
        type: 'amenity',
        factor: 'doorman',
        impact: '+$100-200/month',
        explanation: 'Doorman service adds significant value in NYC'
    });
}
if (amenities.hasAmenity('elevator')) {
    keyAmenities.push('elevator access');
    adjustments.push({
        type: 'amenity',
        factor: 'elevator',
        impact: '+$50-100/month',
        explanation: 'Elevator access increases convenience and value'
    });
}
if (amenities.hasAmenity('washer_dryer')) {
    keyAmenities.push('in-unit laundry');
    adjustments.push({
        type: 'amenity',
        factor: 'in_unit_laundry',
        impact: '+$75-150/month',
        explanation: 'In-unit washer/dryer is highly valued amenity'
    });
}
if (amenities.hasAmenity('outdoor_space')) {
    keyAmenities.push('private outdoor space');
    adjustments.push({
        type: 'amenity',
        factor: 'outdoor_space',
        impact: '+$100-300/month',
        explanation: 'Private outdoor space commands significant premium'
    });
}
if (amenities.hasAmenity('gym')) {
    keyAmenities.push('fitness center');
    adjustments.push({
        type: 'amenity',
        factor: 'gym',
        impact: '+$25-75/month',
        explanation: 'Building gym saves on external membership costs'
    });
}

// LOCATION ANALYSIS
if (location.transitScore >= 8) {
    adjustments.push({
        type: 'location',
        factor: 'prime_location',
        impact: '+$100-250/month premium',
        explanation: 'Prime locations with excellent transit access command premium'
    });
} else if (location.transitScore >= 6) {
    adjustments.push({
        type: 'location',
        factor: 'convenient_location',
        impact: '+$50-150/month premium',
        explanation: 'Good location access adds moderate premium'
    });
}

// ✅ FIXED: Proper discount calculation that handles overpriced properties
const discount = ((marketRent - actualRent) / marketRent * 100);
const monthlySavings = marketRent - actualRent;
const isUndervalued = discount > 0; // Positive discount means undervalued
const isOvervalued = discount < 0;  // Negative discount means overvalued

// ✅ SUCCINCT: More concise analysis based on whether property is under/over/fairly valued
if (isOvervalued) {
    const overvaluedPercent = Math.abs(discount);
    analysis += `This ${beds}BR/${baths}BA apartment in ${neighborhood} is priced at $${actualRent.toLocaleString()}/month, which is ${overvaluedPercent.toFixed(1)}% above the estimated market rent of $${marketRent.toLocaleString()} based on comparable ${buildingType.category.replace('_', ' ')} properties. `;
} else if (isUndervalued) {
    analysis += `This ${beds}BR/${baths}BA apartment in ${neighborhood} offers excellent value at $${actualRent.toLocaleString()}/month, which is ${discount.toFixed(1)}% below the estimated market rent of $${marketRent.toLocaleString()} based on comparable ${buildingType.category.replace('_', ' ')} properties. `;
} else {
    analysis += `This ${beds}BR/${baths}BA apartment in ${neighborhood} is fairly priced at $${actualRent.toLocaleString()}/month, closely matching the estimated market rent of $${marketRent.toLocaleString()} based on comparable ${buildingType.category.replace('_', ' ')} properties. `;
}

// ✅ KEEP: All the building type, condition, amenities, and location analysis code EXACTLY THE SAME
// (Don't change the adjustments array building - keep all that logic)

// ✅ SUCCINCT: Replace the verbose comparable analysis section with this:
const comparableCount = comparables?.length || 0;
if (comparableCount >= 8) {
    confidence = Math.min(95, confidence + 10);
    analysis += `The analysis confidence is high due to extensive comparable data from ${comparableCount} similar properties. `;
} else if (comparableCount >= 5) {
    confidence = Math.min(90, confidence + 5);
    analysis += `The analysis confidence is good based on ${comparableCount} comparable properties. `;
} else if (comparableCount >= 3) {
    analysis += `Based on analysis of ${comparableCount} comparable properties, `;
} else {
    confidence = Math.max(50, confidence - 15);
    analysis += `Based on limited comparable data, confidence is reduced. `;
}

// ✅ SUCCINCT: Replace the verbose value proposition with this concise version
if (isOvervalued) {
    const overvaluedPercent = Math.abs(discount);
    analysis += `Renters should be aware they're paying a premium of approximately $${Math.abs(monthlySavings).toLocaleString()}/month above comparable properties.`;
} else if (discount >= 25) {
    analysis += `This represents exceptional savings of $${monthlySavings.toLocaleString()}/month or $${(monthlySavings * 12).toLocaleString()}/year. The significant undervaluation suggests either rent stabilization controls or an owner seeking quick occupancy.`;
} else if (discount >= 15) {
    analysis += `This represents significant savings of $${monthlySavings.toLocaleString()}/month or $${(monthlySavings * 12).toLocaleString()}/year. The below-market pricing suggests either rent stabilization controls or an owner seeking quick occupancy.`;
} else if (discount >= 10) {
    analysis += `This offers good value with savings of $${monthlySavings.toLocaleString()}/month or $${(monthlySavings * 12).toLocaleString()}/year.`;
} else if (discount > 0) {
    analysis += `This provides fair value with modest savings of $${monthlySavings.toLocaleString()}/month or $${(monthlySavings * 12).toLocaleString()}/year.`;
} else {
    analysis += `This is priced at market rate with no significant savings compared to similar properties.`;
}
    
    return {
        confidence_percentage: confidence,
        analysis_method: methodology,
        detailed_explanation: analysis,
        market_comparison: {
            actual_rent: actualRent,
            estimated_market_rent: marketRent,
            discount_percentage: discount, // Can be negative for overvalued properties
            monthly_savings: monthlySavings, // Can be negative for overvalued properties
            annual_savings: monthlySavings * 12,
            is_undervalued: isUndervalued,
            is_overvalued: isOvervalued
        },
        adjustment_breakdown: adjustments,
        building_analysis: {
            type: buildingType.category,
            condition: condition.level,
            condition_score: condition.score,
            year_built: property.builtIn
        },
        amenity_analysis: {
            total_score: amenities.totalScore,
            key_amenities: keyAmenities,
            amenity_premium: adjustments
                .filter(adj => adj.type === 'amenity')
                .length * 100 // Rough estimate
        },
        location_analysis: {
            neighborhood: property.neighborhood,
            transit_score: location.transitScore,
            walkability: location.walkability,
            desirability: location.desirability
        },
        comparable_properties: {
            count: comparableCount,
            method: methodology,
            reliability: confidence >= 80 ? 'high' : confidence >= 60 ? 'medium' : 'low'
        },
        calculation_methodology: [
            'Identify comparable properties with similar bed/bath configuration',
            'Apply building type and condition adjustments',
            'Factor in amenity premiums and location value',
            'Calculate weighted average market rent',
            'Determine discount percentage and savings potential'
        ]
    };
}

// ===================================================================
// ALL HELPER FUNCTIONS INCLUDED - ADD THESE TO claude-market-analyzer.js
// ===================================================================

calculateBuildingAge(builtIn) {
    if (!builtIn) return { age: null, era: 'unknown', isRentStabilizedEra: false };
    
    const currentYear = new Date().getFullYear();
    const age = currentYear - builtIn;
    
    let era = 'modern';
    let isRentStabilizedEra = false;
    
    if (builtIn >= 1943 && builtIn <= 1946) {
        era = 'emergency_rent_control';
        isRentStabilizedEra = true;
    } else if (builtIn >= 1947 && builtIn <= 1973) {
        era = 'prime_stabilization';
        isRentStabilizedEra = true;
    } else if (builtIn >= 1974 && builtIn <= 1983) {
        era = 'early_stabilization';
        isRentStabilizedEra = true;
    } else if (builtIn >= 1984 && builtIn <= 2000) {
        era = 'post_stabilization';
        isRentStabilizedEra = false;
    } else if (builtIn >= 2001) {
        era = 'modern';
        isRentStabilizedEra = false;
    } else if (builtIn < 1943) {
        era = 'pre_war';
        isRentStabilizedEra = true; // Many pre-war buildings are stabilized
    }
    
    return { age, era, isRentStabilizedEra };
}

analyzeBuildingType(property) {
    const description = (property.description || '').toLowerCase();
    const amenities = (property.amenities || []).join(' ').toLowerCase();
    const fullText = `${description} ${amenities}`;
    
    // Building type indicators
    const luxuryHighRise = ['luxury high-rise', 'luxury tower', 'full service building', 'concierge'];
    const boutiqueLuxury = ['boutique', 'luxury building', 'designer', 'high-end finishes'];
    const traditionalRental = ['rental building', 'apartment building', 'multiple dwelling'];
    const olderWalkUp = ['walk-up', 'walk up', 'no elevator', 'pre-war'];
    
    let category = 'traditional_rental'; // default
    
    if (luxuryHighRise.some(term => fullText.includes(term))) {
        category = 'luxury_high_rise';
    } else if (boutiqueLuxury.some(term => fullText.includes(term))) {
        category = 'boutique_luxury';
    } else if (olderWalkUp.some(term => fullText.includes(term))) {
        category = 'older_walk_up';
    }
    
    return { category };
}

analyzePropertyCondition(property) {
    const description = (property.description || '').toLowerCase();
    const amenities = (property.amenities || []).join(' ').toLowerCase();
    const fullText = `${description} ${amenities}`;
    
    const pristineIndicators = ['gut renovated', 'pristine', 'brand new', 'luxury finishes'];
    const updatedIndicators = ['renovated', 'updated', 'modern', 'new kitchen'];
    const originalIndicators = ['original', 'classic', 'charming', 'pre-war details'];
    const needsWorkIndicators = ['needs work', 'fixer', 'potential', 'bring your architect'];
    
    if (needsWorkIndicators.some(term => fullText.includes(term))) {
        return { level: 'needs_work', score: 2 };
    } else if (pristineIndicators.some(term => fullText.includes(term))) {
        return { level: 'pristine', score: 5 };
    } else if (updatedIndicators.some(term => fullText.includes(term))) {
        return { level: 'updated', score: 4 };
    } else if (originalIndicators.some(term => fullText.includes(term))) {
        return { level: 'original', score: 3 };
    }
    
    return { level: 'standard', score: 3 };
}

analyzeAmenities(property) {
    const amenities = (property.amenities || []).join(' ').toLowerCase();
    const description = (property.description || '').toLowerCase();
    const fullText = `${amenities} ${description}`;
    
    const amenityChecks = {
        doorman: ['doorman', 'concierge'],
        elevator: ['elevator'],
        washer_dryer: ['washer/dryer', 'in-unit laundry'],
        outdoor_space: ['balcony', 'terrace', 'rooftop'],
        gym: ['gym', 'fitness'],
        parking: ['parking', 'garage']
    };
    
    let totalScore = 0;
    const hasAmenity = (amenityType) => {
        const terms = amenityChecks[amenityType] || [];
        return terms.some(term => fullText.includes(term));
    };
    
    Object.keys(amenityChecks).forEach(amenity => {
        if (hasAmenity(amenity)) totalScore += 1;
    });
    
    return { totalScore, hasAmenity };
}

analyzeLocationValue(property) {
    const neighborhood = (property.neighborhood || '').toLowerCase();
    
    // Neighborhood scoring (simplified)
    const premiumNeighborhoods = ['soho', 'tribeca', 'west village'];
    const goodNeighborhoods = ['east village', 'brooklyn heights', 'williamsburg'];
    
    let transitScore = 6; // default
    let desirability = 'medium';
    
    if (premiumNeighborhoods.some(n => neighborhood.includes(n))) {
        transitScore = 9;
        desirability = 'high';
    } else if (goodNeighborhoods.some(n => neighborhood.includes(n))) {
        transitScore = 8;
        desirability = 'high';
    }
    
    return {
        transitScore,
        walkability: transitScore >= 8 ? 'excellent' : 'good',
        desirability
    };
}

estimateUnitCount(property) {
    // Simple heuristic based on building description
    const description = (property.description || '').toLowerCase();
    
    if (description.includes('boutique') || description.includes('small building')) {
        return { estimate: 8, confidence: 'medium' };
    } else if (description.includes('large building') || description.includes('high-rise')) {
        return { estimate: 50, confidence: 'medium' };
    }
    
    return { estimate: 15, confidence: 'low' }; // default estimate
}

analyzeRentLevel(property) {
    // This would need market data - simplified version
    const rent = property.price || property.monthlyRent || 0;
    const bedrooms = property.bedrooms || 0;
    
    // Very basic market comparison (you'd want real market data here)
    const estimatedMarket = bedrooms * 2000 + 1000; // rough estimate
    const variance = ((rent - estimatedMarket) / estimatedMarket) * 100;
    
    let level = 'market_rate';
    if (variance < -10) level = 'below_market';
    if (variance > 10) level = 'above_market';
    
    return { level, variance };
}

/**
 * FIXED: Proper DHCR address matching with real similarity calculation
 * Replace the existing findDHCRMatches function in claude-market-analyzer.js
 */
findDHCRMatches(property, rentStabilizedBuildings) {
    if (!rentStabilizedBuildings || rentStabilizedBuildings.length === 0) {
        return [];
    }
    
    const propertyAddress = this.normalizeAddress(property.address || '');
    if (!propertyAddress) return [];
    
    const matches = [];
    
    for (const building of rentStabilizedBuildings) {
        const buildingAddress = this.normalizeAddress(building.address || '');
        if (!buildingAddress) continue;
        
        // Calculate REAL similarity using the existing function
        const similarity = this.calculateAddressSimilarity(propertyAddress, buildingAddress);
        
        // Only include matches with meaningful similarity (60%+)
        if (similarity >= 0.6) {
            matches.push({
                ...building,
                similarity: similarity // Use ACTUAL calculated similarity
            });
        }
    }
    
    // Sort by similarity (highest first) and return top 3
    return matches
        .sort((a, b) => b.similarity - a.similarity)
        .slice(0, 3);
}

/**
 * ENHANCED: More robust address similarity calculation
 * This improves the existing calculateAddressSimilarity function
 */
calculateAddressSimilarity(addr1, addr2) {
    if (!addr1 || !addr2) return 0;
    
    const words1 = addr1.split(' ').filter(word => word.length > 0);
    const words2 = addr2.split(' ').filter(word => word.length > 0);
    
    if (words1.length === 0 || words2.length === 0) return 0;
    
    // Extract street numbers
    const num1 = words1.find(word => /^\d+$/.test(word));
    const num2 = words2.find(word => /^\d+$/.test(word));
    
    // If street numbers don't match, similarity is very low
    if (num1 && num2 && num1 !== num2) {
        return 0.1; // Very low similarity for different street numbers
    }
    
    // Calculate word overlap
    const intersection = words1.filter(word => words2.includes(word));
    const union = [...new Set([...words1, ...words2])];
    
    if (union.length === 0) return 0;
    
    const basicSimilarity = intersection.length / union.length;
    
    // Boost similarity if street numbers match
    if (num1 && num2 && num1 === num2) {
        return Math.min(1.0, basicSimilarity + 0.3);
    }
    
    return basicSimilarity;
}

/**
 * DEBUG: Add this helper function to see what's actually being compared
 */
debugAddressMatching(property, rentStabilizedBuildings) {
    console.log(`\n🔍 DEBUG: Matching "${property.address}"`);
    
    const propertyAddress = this.normalizeAddress(property.address || '');
    console.log(`   Normalized target: "${propertyAddress}"`);
    
    const matches = rentStabilizedBuildings.slice(0, 5).map(building => {
        const buildingAddress = this.normalizeAddress(building.address || '');
        const similarity = this.calculateAddressSimilarity(propertyAddress, buildingAddress);
        return {
            original: building.address,
            normalized: buildingAddress,
            similarity: Math.round(similarity * 100)
        };
    });
    
    console.log('   Sample comparisons:');
    matches.forEach(match => {
        console.log(`     "${match.original}" → ${match.similarity}% similarity`);
    });
    
    return matches;
}
	    /**
     * ✅ NEW: Analyze description for rent stabilization using pattern recognition
     */
    analyzeDescriptionForStabilization(description) {
        // Enhanced pattern matching for subtle mentions
        const stabilizationPatterns = [
            // Direct mentions
            /rent.{0,5}stabili[sz]ed?/i,
            /stabili[sz]ed.{0,5}rent/i,
            /regulated.{0,10}rent/i,
            /rent.{0,5}controlled?/i,
            
            // Legal/regulatory language
            /subject.{0,10}rent.{0,10}regulat/i,
            /rent.{0,5}protection/i,
            /legal.{0,10}rent/i,
            /dhcr/i,
            /housing.{0,10}preservation/i,
            
            // Tenant protection language
            /long.{0,5}term.{0,5}tenant/i,
            /tenant.{0,5}protection/i,
            /lease.{0,5}renewal.{0,5}right/i,
            /rent.{0,5}increase.{0,5}limit/i,
            
            // Building program indicators
            /mitchell.{0,5}lama/i,
            /j.?51/i,
            /421.?a/i
        ];
        
        let confidence = 0;
        let evidence = [];
        
        for (const pattern of stabilizationPatterns) {
            const match = description.match(pattern);
            if (match) {
                evidence.push(match[0]);
                
                // Weight different patterns
                if (pattern.toString().includes('stabili')) confidence += 40;
                else if (pattern.toString().includes('regulated')) confidence += 35;
                else if (pattern.toString().includes('dhcr')) confidence += 45;
                else if (pattern.toString().includes('controlled')) confidence += 30;
                else confidence += 20;
            }
        }
        
        return {
            hasStabilizationMention: confidence >= 30,
            confidence: Math.min(100, confidence),
            evidence: evidence.join(', '),
            patterns_matched: evidence.length
        };
    }

    /**
     * ✅ NEW: Create standardized response for explicit stabilization mentions
     */
    createExplicitStabilizationResponse(property, buildingAge, unitCount, rentLevel, method, claudeAnalysis = null) {
        const evidence = claudeAnalysis ? claudeAnalysis.evidence : 'Direct mention in description';
        const confidence = claudeAnalysis ? Math.min(100, claudeAnalysis.confidence) : 100;
        
        return {
            confidence_percentage: confidence,
            analysis_method: method,
            key_factors: ['explicit_stabilization_mention'],
            legal_factors: [`Explicit stabilization mention detected: "${evidence}"`],
            detailed_explanation: `The property description contains explicit mention of rent stabilization ("${evidence}"), providing definitive confirmation of the unit's stabilized status. This is the strongest possible indicator of rent stabilization.`,
            building_age_factor: {
                construction_year: property.builtIn,
                era: buildingAge.era,
                years_old: buildingAge.age,
                is_stabilization_era: buildingAge.isRentStabilizedEra
            },
            unit_count_factor: {
                estimated_units: unitCount.estimate,
                meets_minimum: unitCount.estimate >= 6,
                confidence: unitCount.confidence
            },
            rent_level_factor: {
                level: rentLevel.level,
                variance_from_market: rentLevel.variance,
                monthly_rent: property.price || property.monthlyRent
            },
            dhcr_matches: [] // Not needed when explicitly mentioned
        };
    }
}


module.exports = EnhancedClaudeMarketAnalyzer;
