// claude-market-analyzer.js
// ENHANCED CLAUDE-POWERED MARKET ANALYSIS ENGINE - FIXED VERSION
// Provides sophisticated comparative market analysis with comprehensive reasoning
// for both sales and rentals with rent-stabilization detection
require('dotenv').config();
const axios = require('axios');

/**
 * Enhanced Claude-Powered Market Analysis Engine
 * Provides sophisticated comparative market analysis for both sales and rentals
 * with comprehensive reasoning, detailed breakdowns, and investment insights
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
        
        console.log('🤖 Enhanced Claude Market Analyzer initialized');
    }

    /**
     * ENHANCED SALES ANALYSIS with comprehensive reasoning and detailed insights
     */
    async analyzeSalesUndervaluation(targetProperty, comparableProperties, neighborhood, options = {}) {
        const threshold = options.undervaluationThreshold || 10;
        
        console.log(`🤖 Enhanced Claude analyzing sale: ${targetProperty.address}`);
        
        try {
            // Prepare enhanced comparative market data
            const enhancedContext = this.buildEnhancedSalesContext(targetProperty, comparableProperties, neighborhood);
            
            // Get Claude's sophisticated analysis
            const claudeResponse = await this.callClaudeForEnhancedSalesAnalysis(enhancedContext, threshold);
            
            if (!claudeResponse.success) {
                return {
                    isUndervalued: false,
                    discountPercent: 0,
                    estimatedMarketPrice: targetProperty.salePrice,
                    actualPrice: targetProperty.salePrice,
                    confidence: 0,
                    method: 'enhanced_claude_analysis_failed',
                    reasoning: claudeResponse.error || 'Enhanced analysis failed'
                };
            }
            
            const analysis = claudeResponse.analysis;
            
            // Validate enhanced response structure
            if (!this.validateEnhancedSalesAnalysis(analysis)) {
                throw new Error('Invalid enhanced analysis structure from Claude');
            }
            
            // Build comprehensive reasoning
            const comprehensiveReasoning = this.buildComprehensiveSalesReasoning(analysis, targetProperty, enhancedContext);
            
            console.log(`   💰 Claude enhanced estimate: $${analysis.estimatedMarketPrice.toLocaleString()}`);
            console.log(`   📊 Discount: ${analysis.discountPercent.toFixed(1)}%`);
            console.log(`   ✅ Confidence: ${analysis.confidence}%`);
            console.log(`   🎯 Deal quality: ${analysis.dealQuality || 'fair'} (${analysis.score || 50}/100)`);
            
            return {
                isUndervalued: analysis.discountPercent >= threshold && analysis.confidence >= 60,
                discountPercent: analysis.discountPercent,
                estimatedMarketPrice: analysis.estimatedMarketPrice,
                actualPrice: targetProperty.salePrice,
                potentialProfit: analysis.estimatedMarketPrice - targetProperty.salePrice,
                confidence: analysis.confidence,
                method: 'enhanced_claude_comparative_analysis',
                comparablesUsed: comparableProperties.length,
                adjustmentBreakdown: analysis.adjustmentBreakdown || {},
                reasoning: comprehensiveReasoning,
                score: analysis.score,
                dealQuality: analysis.dealQuality,
                // Enhanced metrics
                detailedAnalysis: analysis.detailedAnalysis || {},
                keyMetrics: analysis.keyMetrics || {},
                comparableInsights: analysis.comparableInsights || {},
                investmentRating: analysis.investmentRating || 'B',
                marketPosition: analysis.marketPosition || 'mid-market',
            };
            
        } catch (error) {
            console.warn(`   ⚠️ Enhanced Claude analysis error: ${error.message}`);
            return {
                isUndervalued: false,
                discountPercent: 0,
                estimatedMarketPrice: targetProperty.salePrice,
                actualPrice: targetProperty.salePrice,
                confidence: 0,
                method: 'enhanced_claude_analysis_failed',
                reasoning: `Enhanced analysis failed: ${error.message}`
            };
        }
    }

    /**
     * ENHANCED RENTALS ANALYSIS with rent stabilization detection
     */
    async analyzeRentalsUndervaluation(targetProperty, comparableProperties, neighborhood, options = {}) {
        const threshold = options.undervaluationThreshold || 15;
        
        console.log(`🤖 Enhanced Claude analyzing rental: ${targetProperty.address}`);
        
        try {
            // Prepare enhanced context with rent stabilization data
            const enhancedContext = this.buildEnhancedRentalsContext(targetProperty, comparableProperties, neighborhood, options);
            
            // Get Claude's sophisticated analysis
            const claudeResponse = await this.callClaudeForEnhancedRentalsAnalysis(enhancedContext, threshold);
            
            if (!claudeResponse.success) {
                return {
                    isUndervalued: false,
                    percentBelowMarket: 0,
                    estimatedMarketRent: targetProperty.price,
                    actualRent: targetProperty.price,
                    confidence: 0,
                    method: 'enhanced_claude_analysis_failed',
                    reasoning: claudeResponse.error || 'Enhanced analysis failed',
                    rentStabilizedProbability: 0,
                    rentStabilizedFactors: [],
                    rentStabilizedExplanation: 'Analysis failed'
                };
            }
            
            const analysis = claudeResponse.analysis;
            
            // Validate enhanced response structure
            if (!this.validateEnhancedRentalsAnalysis(analysis)) {
                throw new Error('Invalid enhanced analysis structure from Claude');
            }
            
            // Build comprehensive reasoning
            const comprehensiveReasoning = this.buildComprehensiveRentalsReasoning(analysis, targetProperty, enhancedContext);
            
            console.log(`   💰 Claude enhanced estimate: $${analysis.estimatedMarketRent?.toLocaleString()}/month`);
            console.log(`   📊 Market position: ${analysis.percentBelowMarket?.toFixed(1)}%`);
            console.log(`   🔒 Rent stabilized: ${analysis.rentStabilizedProbability || 0}%`);
            console.log(`   ✅ Confidence: ${analysis.confidence}%`);
            
            return {
                isUndervalued: analysis.percentBelowMarket >= threshold && analysis.confidence >= 60,
                percentBelowMarket: analysis.percentBelowMarket || 0,
                estimatedMarketRent: analysis.estimatedMarketRent || targetProperty.price,
                actualRent: targetProperty.price,
                potentialSavings: analysis.potentialMonthlySavings || 0,
                confidence: analysis.confidence || 0,
                method: 'enhanced_claude_comparative_analysis',
                comparablesUsed: comparableProperties.length,
                reasoning: comprehensiveReasoning,
                undervaluationConfidence: analysis.undervaluationConfidence || analysis.confidence || 0,
                
                // Rent stabilization analysis
                rentStabilizedProbability: analysis.rentStabilizedProbability || 0,
                rentStabilizedFactors: analysis.rentStabilizedFactors || [],
                rentStabilizedExplanation: analysis.rentStabilizedExplanation || 'No stabilization indicators found',
                
                // Enhanced metrics
                detailedAnalysis: analysis.detailedAnalysis || {},
                keyMetrics: analysis.keyMetrics || {},
                comparableInsights: analysis.comparableInsights || {},
                legalProtectionValue: analysis.legalProtectionValue || 0,
                investmentMerit: analysis.investmentMerit || 'low'
            };
            
        } catch (error) {
            console.warn(`   ⚠️ Enhanced Claude analysis error: ${error.message}`);
            return {
                isUndervalued: false,
                percentBelowMarket: 0,
                estimatedMarketRent: targetProperty.price,
                actualRent: targetProperty.price,
                confidence: 0,
                method: 'enhanced_claude_analysis_failed',
                reasoning: `Enhanced analysis failed: ${error.message}`,
                rentStabilizedProbability: 0,
                rentStabilizedFactors: [],
                rentStabilizedExplanation: 'Analysis failed'
            };
        }
    }

    /**
     * Core Claude API call with enhanced error handling and retry logic
     * FIXED: Improved JSON parsing and error handling
     */
    async callClaude(systemPrompt, userPrompt, analysisType) {
        const maxRetries = 3;
        let attempt = 0;
        
        while (attempt < maxRetries) {
            try {
                this.apiCallsUsed++;
                console.log(`   🤖 Enhanced Claude API call #${this.apiCallsUsed} (${analysisType}, attempt ${attempt + 1})`);
                
                const response = await axios.post(
                    'https://api.anthropic.com/v1/messages',
                    {
                        model: 'claude-3-haiku-20240307',
                        max_tokens: 2000,
                        temperature: 0.05,
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
                console.log(`   ✅ Enhanced Claude response received (${responseText.length} chars)`);
                
                // FIXED: Enhanced JSON parsing with better error handling
                try {
                    // First, try to extract JSON from the response
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
                    console.warn(`   ⚠️ JSON parse error, attempting enhanced extraction: ${parseError.message}`);
                    
                    // Fallback: extract key-value pairs manually
                    const extractedData = this.extractDataFromResponse(responseText);
                    if (extractedData && Object.keys(extractedData).length > 3) {
                        return { success: true, analysis: extractedData };
                    }
                    
                    throw new Error('Could not parse enhanced Claude response as JSON');
                }
                
            } catch (error) {
                attempt++;
                console.warn(`   ⚠️ Enhanced Claude API error (attempt ${attempt}): ${error.message}`);
                
                if (attempt >= maxRetries) {
                    return { 
                        success: false, 
                        error: `Enhanced analysis failed after ${maxRetries} attempts: ${error.message}` 
                    };
                }
                
                // Exponential backoff
                await this.delay(3000 * Math.pow(2, attempt - 1));
            }
        }
    }

    /**
     * FIXED: Enhanced data extraction from malformed responses
     */
    extractDataFromResponse(responseText) {
        try {
            const extracted = {};
            
            // Extract key numerical values using regex patterns
            const patterns = {
                estimatedMarketPrice: /(?:estimatedMarketPrice|estimated.*price|market.*price)[\s\S]*?(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)/i,
                estimatedMarketRent: /(?:estimatedMarketRent|estimated.*rent|market.*rent)[\s\S]*?(\d{1,3}(?:,\d{3})*)/i,
                discountPercent: /(?:discountPercent|discount|below.*market)[\s\S]*?(-?\d+(?:\.\d+)?)/i,
                percentBelowMarket: /(?:percentBelowMarket|percent.*below|market.*position)[\s\S]*?(-?\d+(?:\.\d+)?)/i,
                confidence: /(?:confidence|certainty)[\s\S]*?(\d+(?:\.\d+)?)/i,
                rentStabilizedProbability: /(?:rentStabilizedProbability|rent.*stabilized|stabilization)[\s\S]*?(\d+(?:\.\d+)?)/i,
                score: /(?:score|rating)[\s\S]*?(\d+(?:\.\d+)?)/i
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
            
            // Extract string values
            const stringPatterns = {
                reasoning: /(?:reasoning|analysis)[\s\S]*?[":]\s*["']?([^"'}\n]{20,200})/i,
                dealQuality: /(?:dealQuality|deal.*quality)[\s\S]*?[":]\s*["']?(excellent|good|fair|poor|best)/i,
                rentStabilizedExplanation: /(?:rentStabilizedExplanation|stabilization.*explanation)[\s\S]*?[":]\s*["']?([^"'}\n]{10,150})/i
            };
            
            for (const [key, pattern] of Object.entries(stringPatterns)) {
                const match = responseText.match(pattern);
                if (match) {
                    extracted[key] = match[1].trim();
                }
            }
            
            // Set reasonable defaults if we extracted some data
            if (Object.keys(extracted).length > 0) {
                extracted.confidence = extracted.confidence || 50;
                extracted.reasoning = extracted.reasoning || 'Analysis based on extracted data';
                extracted.rentStabilizedFactors = [];
                extracted.rentStabilizedExplanation = extracted.rentStabilizedExplanation || 'No specific indicators found';
            }
            
            return Object.keys(extracted).length > 3 ? extracted : null;
            
        } catch (error) {
            console.warn(`   ⚠️ Data extraction failed: ${error.message}`);
            return null;
        }
    }

    /**
     * Build enhanced sales context for Claude analysis
     */
    buildEnhancedSalesContext(targetProperty, comparableProperties, neighborhood) {
        // FIXED: Ensure targetProperty is properly referenced
        const target = {
            address: targetProperty.address || 'Unknown Address',
            salePrice: targetProperty.salePrice || targetProperty.price || 0,
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

        const marketStats = this.calculateMarketStatistics(comparableProperties, 'sales');
        const comparableAnalysis = this.analyzeComparables(target, comparableProperties, 'sales');
        const neighborhoodAnalysis = this.analyzeNeighborhood(neighborhood, 'sales');

        return {
            targetProperty: target,
            marketStats,
            comparables: comparableAnalysis.topComparables,
            comparableAnalysis,
            neighborhood: neighborhoodAnalysis,
            totalComparables: comparableProperties.length
        };
    }

    /**
     * Build enhanced rentals context for Claude analysis
     */
    buildEnhancedRentalsContext(targetProperty, comparableProperties, neighborhood, options = {}) {
        // FIXED: Ensure targetProperty is properly referenced
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
            pricePerSqft: targetProperty.sqft > 0 ? (targetProperty.price || 0) / targetProperty.sqft : null,
            analysis: {
                amenityScore: this.calculateAmenityScore(targetProperty.amenities || []),
                conditionScore: this.assessConditionFromDescription(targetProperty.description || '')
            }
        };

        const marketStats = this.calculateMarketStatistics(comparableProperties, 'rentals');
        const comparableAnalysis = this.analyzeComparables(target, comparableProperties, 'rentals');
        const neighborhoodAnalysis = this.analyzeNeighborhood(neighborhood, 'rentals');
        const rentStabilizationContext = this.buildRentStabilizationContext(target, options.rentStabilizedBuildings || []);

        return {
            targetProperty: target,
            marketStats,
            comparables: comparableAnalysis.topComparables,
            comparableAnalysis,
            neighborhood: neighborhoodAnalysis,
            rentStabilizationContext,
            totalComparables: comparableProperties.length
        };
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
     * Calculate market statistics for comparables
     */
    calculateMarketStatistics(comparables, type) {
        if (!comparables || comparables.length === 0) {
            return { priceStats: {}, psfStats: {} };
        }
        
        const priceField = type === 'sales' ? 'salePrice' : 'price';
        const prices = comparables.map(comp => comp[priceField] || comp.price || 0).filter(p => p > 0);
        const psfs = comparables.map(comp => comp.pricePerSqft || (comp.sqft > 0 ? (comp[priceField] || comp.price || 0) / comp.sqft : 0)).filter(p => p > 0);
        
        const calculateStats = (values) => {
            if (values.length === 0) return {};
            const sorted = values.slice().sort((a, b) => a - b);
            const sum = values.reduce((a, b) => a + b, 0);
            const mean = sum / values.length;
            const variance = values.reduce((acc, val) => acc + Math.pow(val - mean, 2), 0) / values.length;
            
            return {
                min: sorted[0],
                max: sorted[sorted.length - 1],
                median: sorted[Math.floor(sorted.length / 2)],
                mean: Math.round(mean),
                stdDev: Math.round(Math.sqrt(variance)),
                q1: sorted[Math.floor(sorted.length * 0.25)],
                q3: sorted[Math.floor(sorted.length * 0.75)]
            };
        };
        
        return {
            priceStats: calculateStats(prices),
            psfStats: calculateStats(psfs)
        };
    }

    /**
     * Analyze comparables and create similarity rankings
     */
    analyzeComparables(targetProperty, comparables, type) {
        const scoredComparables = comparables.map(comp => ({
            ...comp,
            similarity: this.calculatePropertySimilarity(targetProperty, comp, type)
        })).sort((a, b) => b.similarity - a.similarity);
        
        // Create analysis tiers
        const tiers = [
            {
                description: 'Exact matches (same bed/bath)',
                properties: scoredComparables.filter(c => c.bedrooms === targetProperty.bedrooms && c.bathrooms === targetProperty.bathrooms),
                count: 0,
                avgPrice: 0,
                avgPsf: 0
            },
            {
                description: 'Close matches (±1 bed/bath)',
                properties: scoredComparables.filter(c => 
                    Math.abs(c.bedrooms - targetProperty.bedrooms) <= 1 && 
                    Math.abs(c.bathrooms - targetProperty.bathrooms) <= 0.5
                ),
                count: 0,
                avgPrice: 0,
                avgPsf: 0
            },
            {
                description: 'Similar properties',
                properties: scoredComparables.filter(c => c.similarity >= 6),
                count: 0,
                avgPrice: 0,
                avgPsf: 0
            }
        ];
        
        // Calculate tier statistics
        tiers.forEach(tier => {
            tier.count = tier.properties.length;
            if (tier.count > 0) {
                const priceField = type === 'sales' ? 'salePrice' : 'price';
                const prices = tier.properties.map(p => p[priceField] || p.price || 0).filter(p => p > 0);
                const psfs = tier.properties.map(p => p.pricePerSqft || 0).filter(p => p > 0);
                
                tier.avgPrice = prices.length > 0 ? Math.round(prices.reduce((a, b) => a + b, 0) / prices.length) : 0;
                tier.avgPsf = psfs.length > 0 ? psfs.reduce((a, b) => a + b, 0) / psfs.length : 0;
            }
        });
        
        return {
            topComparables: scoredComparables.slice(0, 12),
            tiers: tiers.filter(t => t.count > 0)
        };
    }

    /**
     * Calculate property similarity score (0-10)
     */
    calculatePropertySimilarity(target, comparable, type) {
        let score = 0;
        
        // Bedroom/bathroom match (40% of score)
        if (target.bedrooms === comparable.bedrooms) score += 2;
        else if (Math.abs(target.bedrooms - comparable.bedrooms) === 1) score += 1;
        
        if (Math.abs(target.bathrooms - comparable.bathrooms) <= 0.5) score += 2;
        else if (Math.abs(target.bathrooms - comparable.bathrooms) <= 1) score += 1;
        
        // Square footage similarity (20% of score)
        if (target.sqft > 0 && comparable.sqft > 0) {
            const sqftDiff = Math.abs(target.sqft - comparable.sqft) / target.sqft;
            if (sqftDiff <= 0.1) score += 2;
            else if (sqftDiff <= 0.2) score += 1.5;
            else if (sqftDiff <= 0.3) score += 1;
        }
        
        // Building age similarity (10% of score)
        if (target.builtIn && comparable.builtIn) {
            const ageDiff = Math.abs(target.builtIn - comparable.builtIn);
            if (ageDiff <= 5) score += 1;
            else if (ageDiff <= 15) score += 0.5;
        }
        
        // Amenity overlap (20% of score)
        const targetAmenities = new Set(target.amenities || []);
        const comparableAmenities = new Set(comparable.amenities || []);
        const intersection = new Set([...targetAmenities].filter(x => comparableAmenities.has(x)));
        const union = new Set([...targetAmenities, ...comparableAmenities]);
        
        if (union.size > 0) {
            score += (intersection.size / union.size) * 2;
        }
        
        // Price range reasonableness (10% of score)
        const priceField = type === 'sales' ? 'salePrice' : 'price';
        const targetPrice = target[priceField] || target.price || 0;
        const compPrice = comparable[priceField] || comparable.price || 0;
        
        if (targetPrice > 0 && compPrice > 0) {
            const priceDiff = Math.abs(targetPrice - compPrice) / targetPrice;
            if (priceDiff <= 0.3) score += 1;
            else if (priceDiff <= 0.5) score += 0.5;
        }
        
        return Math.min(10, Math.max(0, score));
    }

    /**
     * Validation functions
     */
    validateEnhancedSalesAnalysis(analysis) {
        return analysis && 
               typeof analysis.estimatedMarketPrice === 'number' &&
               typeof analysis.discountPercent === 'number' &&
               typeof analysis.confidence === 'number' &&
               analysis.confidence >= 0 && analysis.confidence <= 100;
    }

    validateEnhancedRentalsAnalysis(analysis) {
        return analysis && 
               (typeof analysis.estimatedMarketRent === 'number' || typeof analysis.estimatedMarketPrice === 'number') &&
               typeof analysis.percentBelowMarket === 'number' &&
               typeof analysis.confidence === 'number' &&
               analysis.confidence >= 0 && analysis.confidence <= 100 &&
               typeof analysis.rentStabilizedProbability === 'number';
    }

    /**
     * Helper functions for rent stabilization context
     */
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

    calculateAmenityScore(amenities) {
        const standardAmenities = [
            'doorman', 'elevator', 'laundry', 'gym', 'rooftop', 'parking',
            'dishwasher', 'air-conditioning', 'hardwood-floors', 'balcony'
        ];
        
        let score = 0;
        for (const amenity of amenities) {
            const normalizedAmenity = amenity.toLowerCase();
            if (standardAmenities.some(std => normalizedAmenity.includes(std))) {
                score += 10;
            }
        }
        
        return Math.min(100, score);
    }

    assessConditionFromDescription(description) {
        const desc = description.toLowerCase();
        let score = 50; // Base score
        
        // Positive indicators
        if (desc.includes('renovated') || desc.includes('updated')) score += 20;
        if (desc.includes('new') || desc.includes('modern')) score += 15;
        if (desc.includes('luxury') || desc.includes('high-end')) score += 15;
        if (desc.includes('pristine') || desc.includes('immaculate')) score += 10;
        
        // Negative indicators
        if (desc.includes('needs work') || desc.includes('fixer')) score -= 30;
        if (desc.includes('original') && !desc.includes('restored')) score -= 10;
        
        return Math.min(100, Math.max(0, score));
    }

    /**
     * ENHANCED SYSTEM PROMPT FOR SALES ANALYSIS with sophisticated reasoning
     */
    buildEnhancedSalesSystemPrompt() {
        return `You are a world-class NYC real estate investment analyst with deep expertise in micro-market pricing, building characteristics, and sophisticated comparative market analysis. Your analysis must be exceptionally detailed, data-driven, and provide actionable investment insights.

ENHANCED ANALYSIS FRAMEWORK:
1. Comprehensive market positioning with percentile rankings
2. Detailed comparable analysis with similarity weighting
3. Investment merit assessment with ROI projections
4. Risk factor identification and mitigation strategies
5. Market timing and velocity considerations
6. Building quality and amenity valuation
7. Neighborhood tier adjustments and premium/discount analysis
8. Financial modeling with cash flow projections
9. Detailed confidence scoring with supporting rationale
10. Specific investment recommendations with risk-adjusted returns

ANALYSIS METHODOLOGY:
1. Identify exact comparable matches by bed/bath in same neighborhood
2. Weight comparables by similarity score (location, building type, amenities)
3. Apply neighborhood tier adjustment to establish base market range
4. Factor building type premium/discount based on construction era
5. Add/subtract amenity values using borough-specific pricing
6. Apply condition adjustments based on description analysis
7. Consider market timing and velocity factors
8. Calculate investment metrics (potential ROI, cash-on-cash returns)
9. Assess risk factors and market positioning
10. Provide detailed confidence scoring with supporting rationale

REQUIRED RESPONSE FORMAT (JSON only):
{
  "estimatedMarketPrice": number,
  "discountPercent": number,
  "confidence": number,
  "score": number,
  "dealQuality": "best|excellent|good|fair|marginal",
  "baseMarketPrice": number,
  "adjustmentBreakdown": {
    "neighborhoodTier": number,
    "buildingType": number,
    "condition": number,
    "amenities": number,
    "marketTiming": number,
    "location": number
  },
  "reasoning": "Comprehensive 3-4 sentence analysis with specific data points",
  "detailedAnalysis": {
    "priceAnalysis": "Detailed price positioning with percentiles and comparables",
    "amenityAnalysis": "Comprehensive amenity value assessment with specifics",
    "marketComparison": "Market tier positioning and competitive analysis",
    "investmentMerit": "Investment opportunity assessment with ROI projections",
    "riskFactors": "Specific risks and mitigation strategies"
  },
  "keyMetrics": {
    "pricePercentile": number,
    "amenityScore": number,
    "marketVelocityScore": number,
    "neighborhoodDesirability": number,
    "buildingQuality": number,
    "investmentGrade": "A+|A|B+|B|C+|C|D"
  },
  "comparableInsights": {
    "bestComparables": ["top 3 most similar addresses"],
    "priceRange": "price range of most similar properties",
    "marketSegment": "ultra-luxury|luxury|mid-luxury|mid-market|value",
    "competitivePosition": "premium|competitive|value|discount"
  },
  "investmentAnalysis": {
    "roi": number,
    "paybackPeriod": number,
    "marketAppreciation": "strong|moderate|weak",
    "liquidityScore": number,
    "cashFlow": "positive|neutral|negative"
  },
  "riskFactors": ["specific risk factor 1", "risk factor 2"],
  "marketPosition": "ultra-luxury|luxury|mid-market|value|distressed"
}

Be exceptionally detailed, specific, and data-driven. Include exact figures, percentiles, and actionable insights.`;
    }

    /**
     * ENHANCED SYSTEM PROMPT FOR RENTALS ANALYSIS with rent stabilization detection
     */
    buildEnhancedRentalsSystemPrompt() {
        return `You are an expert NYC rental market analyst and rent stabilization authority with deep knowledge of micro-market pricing, legal frameworks, and investment analysis. Your expertise spans comparative market analysis, tenant protection laws, and sophisticated investment evaluation.

CONSUMER-FOCUSED ANALYSIS REQUIREMENTS:
- Frame savings in terms of monthly budget relief, not investment returns
- Explain undervaluation using specific market factors renters understand
- Explain undervaluation in terms renters understand (building age, fewer amenities, location factors)
- Use comparative language: "saves you $X/month vs similar apartments"
- Highlight specific amenities and features that justify or contradict the price
- Mention neighborhood positioning in renter-friendly terms
- Explain confidence based on market data quality, not investment risk

RENT STABILIZATION ANALYSIS METHODOLOGY:
1. Building age analysis (pre-1974 buildings have higher probability)
2. Unit count assessment (6+ units often indicate rent stabilization)
3. Rent level evaluation (below-market rents suggest stabilization)
4. Building type classification (walk-ups vs high-rises)
5. Neighborhood rent control patterns
6. Legal protection indicators from description/amenities
7. Market positioning relative to comparable stabilized units
8. Long-term tenant protection value calculation

REQUIRED RESPONSE FORMAT (JSON only):
{
  "estimatedMarketRent": number,
  "percentBelowMarket": number,
  "confidence": number,
  "undervaluationConfidence": number,
  "rentStabilizedProbability": number,
  "rentStabilizedFactors": ["factor1", "factor2", "factor3"],
  "rentStabilizedExplanation": "Clear explanation of tenant protection benefits and rent stabilization indicators",
  "potentialSavings": number,
  "reasoning": "Consumer-focused 3-4 sentence explanation with specific dollar savings and market comparisons showing exactly why this rental offers value",
  "detailedAnalysis": {
    "priceAnalysis": "Rent positioning vs neighborhood average with specific comparable prices",
    "amenityAnalysis": "What amenities you get vs typical properties at this price point", 
    "marketComparison": "How this compares to similar rentals in the area with price examples",
    "stabilizationAnalysis": "Tenant protection benefits and rent increase limitations if stabilized",
    "valueOpportunity": "Why this rental offers better value than typical market options"
  },
  "keyMetrics": {
    "rentPercentile": number,
    "amenityScore": number,
    "buildingQuality": number,
    "tenantProtectionScore": number,
    "neighborhoodValue": number,
    "stabilizationConfidence": number
  },
  "comparableInsights": {
    "bestComparables": ["top 3 most similar addresses with their rents"],
    "rentRange": "rent range of most similar properties",
    "marketSegment": "luxury|mid-luxury|mid-market|value|affordable", 
    "valuePosition": "exceptional_deal|great_value|fair_price|market_rate|overpriced"
  },
  "tenantBenefits": ["specific tenant benefit 1", "benefit 2"],
  "potentialConcerns": ["potential concern 1", "concern 2"]
}

REASONING EXAMPLES:
"This $4,500/month rental saves you approximately $1,500/month compared to similar 2BR apartments in DUMBO, which typically rent for $6,000-$6,500. The below-market pricing is due to the building's 1950s construction and basic amenities, but you still get prime waterfront location access. With 65% rent stabilization probability, you may gain protection from steep rent increases typical in this rapidly gentrifying neighborhood."

"At $3,200/month, this 1BR is priced 28% below the neighborhood average of $4,400 for comparable units. The value stems from being in a pre-war building with original fixtures, trading modern amenities for significant monthly savings. The building's age and unit count suggest possible rent stabilization, which could limit future rent increases to 2-3% annually instead of market-rate jumps."

Focus on tenant value, monthly savings opportunities, and neighborhood positioning from a renter's perspective.`
    }

    /**
     * Build enhanced user prompt for sales analysis
     */
    buildEnhancedSalesUserPrompt(enhancedContext, threshold) {
        const target = enhancedContext.targetProperty;
        const market = enhancedContext.marketStats;
        const neighborhood = enhancedContext.neighborhood;
        
        return `Analyze this NYC property for comprehensive market positioning and investment merit:

TARGET PROPERTY DETAILS:
Address: ${target.address}
Sale Price: $${target.salePrice.toLocaleString()}
Price/sqft: $${target.pricePerSqft?.toFixed(2) || 'N/A'}
Layout: ${target.bedrooms}BR/${target.bathrooms}BA
Square Feet: ${target.sqft || 'Not listed'}
Built: ${target.builtIn || 'Unknown'}
Property Type: ${target.propertyType}
Neighborhood: ${target.neighborhood} (${target.borough})
Monthly HOA: $${target.monthlyHoa.toLocaleString()}
Monthly Tax: $${target.monthlyTax.toLocaleString()}
Amenities: ${target.amenities.join(', ') || 'None listed'}
Description: ${target.description}

ENHANCED MARKET CONTEXT:
Total Comparables: ${enhancedContext.totalComparables}
Median Price: $${market.priceStats.median?.toLocaleString()}
Mean Price: $${market.priceStats.mean?.toLocaleString()}
Price Range: $${market.priceStats.min?.toLocaleString()} - $${market.priceStats.max?.toLocaleString()}
Standard Deviation: $${market.priceStats.stdDev?.toLocaleString()}
Q1 (25th percentile): $${market.priceStats.q1?.toLocaleString()}
Q3 (75th percentile): $${market.priceStats.q3?.toLocaleString()}

PRICE PER SQFT ANALYSIS:
Median PSF: $${market.psfStats.median?.toFixed(2)}
Mean PSF: $${market.psfStats.mean?.toFixed(2)}
PSF Range: $${market.psfStats.min?.toFixed(2)} - $${market.psfStats.max?.toFixed(2)}
PSF Std Dev: $${market.psfStats.stdDev?.toFixed(2)}

TOP COMPARABLE SALES (by similarity):
${enhancedContext.comparables.slice(0, 8).map((comp, i) => 
  `${i+1}. ${comp.address} - ${comp.salePrice?.toLocaleString() || comp.price?.toLocaleString()} | ${comp.bedrooms}BR/${comp.bathrooms}BA | ${comp.sqft || 'N/A'} sqft | ${comp.pricePerSqft?.toFixed(2) || 'N/A'}/sqft | Similarity: ${comp.similarity?.toFixed(1) || 'N/A'}/10 | Built: ${comp.builtIn || 'N/A'} | Amenities: ${comp.amenities?.slice(0, 3).join(', ') || 'None'}`
).join('\n')}

NEIGHBORHOOD ANALYSIS:
Market Tier: ${neighborhood.tier}
Desirability Score: ${neighborhood.desirabilityScore}/10
Price Premium: ${neighborhood.pricePremium}
Investment Outlook: ${neighborhood.investmentOutlook}
Market Velocity: ${neighborhood.velocity}

COMPARABLE ANALYSIS TIERS:
${enhancedContext.comparableAnalysis.tiers.map((tier, i) => 
  `Tier ${i+1} (${tier.description}): ${tier.count} properties, Avg: ${tier.avgPrice?.toLocaleString()}, Avg PSF: ${tier.avgPsf?.toFixed(2)}`
).join('\n')}

ANALYSIS REQUIREMENTS:
- Determine if property is ${threshold}%+ below sophisticated market valuation
- Provide percentile ranking within neighborhood and building type
- Calculate comprehensive investment merit and ROI potential
- Assess market positioning and competitive advantages
- Identify specific value drivers and risk factors
- Provide detailed confidence scoring with supporting data

Return comprehensive analysis as JSON only.`;
    }

    /**
     * Build enhanced user prompt for rentals analysis
     */
    buildEnhancedRentalsUserPrompt(enhancedContext, threshold) {
        const target = enhancedContext.targetProperty;
        const market = enhancedContext.marketStats;
        const neighborhood = enhancedContext.neighborhood;
        const rsContext = enhancedContext.rentStabilizationContext;
        
        return `Analyze this NYC rental property for comprehensive market positioning and rent stabilization assessment:

TARGET PROPERTY DETAILS:
Address: ${target.address}
Current Rent: ${target.price.toLocaleString()}/month
Rent/sqft: ${target.pricePerSqft?.toFixed(2) || 'N/A'}
Layout: ${target.bedrooms}BR/${target.bathrooms}BA
Square Feet: ${target.sqft || 'Not listed'}
Built: ${target.builtIn || 'Unknown'}
Neighborhood: ${target.neighborhood} (${target.borough})
No Fee: ${target.noFee ? 'YES' : 'NO'}
Amenities: ${target.amenities.join(', ') || 'None listed'}
Amenity Score: ${target.analysis.amenityScore}/100
Condition Score: ${target.analysis.conditionScore}/100
Description: ${target.description}

ENHANCED MARKET CONTEXT:
Total Comparables: ${enhancedContext.totalComparables}
Median Rent: ${market.priceStats.median?.toLocaleString()}/month
Mean Rent: ${market.priceStats.mean?.toLocaleString()}/month
Rent Range: ${market.priceStats.min?.toLocaleString()} - ${market.priceStats.max?.toLocaleString()}/month
Standard Deviation: ${market.priceStats.stdDev?.toLocaleString()}
Q1 (25th percentile): ${market.priceStats.q1?.toLocaleString()}/month
Q3 (75th percentile): ${market.priceStats.q3?.toLocaleString()}/month

RENT PER SQFT ANALYSIS:
Median Rent PSF: ${market.psfStats.median?.toFixed(2)}
Mean Rent PSF: ${market.psfStats.mean?.toFixed(2)}
PSF Range: ${market.psfStats.min?.toFixed(2)} - ${market.psfStats.max?.toFixed(2)}
PSF Std Dev: ${market.psfStats.stdDev?.toFixed(2)}

TOP COMPARABLE RENTALS (by similarity):
${enhancedContext.comparables.slice(0, 8).map((comp, i) => 
  `${i+1}. ${comp.address} - ${comp.price?.toLocaleString()}/month | ${comp.bedrooms}BR/${comp.bathrooms}BA | ${comp.sqft || 'N/A'} sqft | ${comp.pricePerSqft?.toFixed(2) || 'N/A'}/sqft | No Fee: ${comp.noFee ? 'YES' : 'NO'} | Similarity: ${comp.similarity?.toFixed(1) || 'N/A'}/10 | Amenities: ${comp.amenities?.slice(0, 5).join(', ') || 'None'}`
).join('\n')}

NEIGHBORHOOD ANALYSIS:
Market Tier: ${neighborhood.tier}
Desirability Score: ${neighborhood.desirabilityScore}/10
Rent Premium: ${neighborhood.rentPremium}
Typical Tenant Profile: ${neighborhood.tenantProfile}
Market Velocity: ${neighborhood.velocity}
Rental Market Outlook: ${neighborhood.rentalOutlook}

RENT STABILIZATION CONTEXT:
Building Database Matches: ${rsContext.buildingMatches.length}
Strongest Match: ${rsContext.strongestMatch ? 
  `${rsContext.strongestMatch.address} (${rsContext.strongestMatch.confidence}% confidence)` : 'None found'}
Building Age Factor: ${rsContext.buildingAgeFactor}
Unit Count Factor: ${rsContext.unitCountFactor}
Rent Level Factor: ${rsContext.rentLevelFactor}
Legal Indicators: ${rsContext.legalIndicators.join(', ') || 'None identified'}

COMPARABLE ANALYSIS TIERS:
${enhancedContext.comparableAnalysis.tiers.map((tier, i) => 
  `Tier ${i+1} (${tier.description}): ${tier.count} properties, Avg: ${tier.avgPrice?.toLocaleString()}/month, Avg PSF: ${tier.avgPsf?.toFixed(2)}`
).join('\n')}

ANALYSIS REQUIREMENTS:
- Determine if rent is ${threshold}%+ below sophisticated market valuation
- Provide percentile ranking within neighborhood and building type
- Calculate comprehensive rent stabilization probability (0-100%)
- Assess legal indicators and protection value
- Analyze investment merit and cash flow potential
- Identify specific value drivers and tenant protection benefits
- Provide detailed confidence scoring across multiple factors

Return comprehensive analysis as JSON only.`;
    }

    /**
     * Call Claude API for enhanced sales analysis
     */
    async callClaudeForEnhancedSalesAnalysis(enhancedContext, threshold) {
        const systemPrompt = this.buildEnhancedSalesSystemPrompt();
        const userPrompt = this.buildEnhancedSalesUserPrompt(enhancedContext, threshold);
        
        return await this.callClaude(systemPrompt, userPrompt, 'enhanced_sales');
    }

    /**
     * Call Claude API for enhanced rentals analysis
     */
    async callClaudeForEnhancedRentalsAnalysis(enhancedContext, threshold) {
        const systemPrompt = this.buildEnhancedRentalsSystemPrompt();
        const userPrompt = this.buildEnhancedRentalsUserPrompt(enhancedContext, threshold);
        
        return await this.callClaude(systemPrompt, userPrompt, 'enhanced_rentals');
    }

    /**
     * Build comprehensive sales reasoning from Claude's detailed analysis
     */
    buildComprehensiveSalesReasoning(analysis, targetProperty, context) {
        const segments = [];
        
        // Market positioning with specific data
        if (analysis.keyMetrics?.pricePercentile) {
            segments.push(
                `This property is priced ${analysis.discountPercent}% below its estimated market value of ${analysis.estimatedMarketPrice?.toLocaleString()}, ` +
                `making it one of the better deals in ${targetProperty.neighborhood} (better value than ${100 - analysis.keyMetrics.pricePercentile}% of similar properties).`
            );
        } else {
            segments.push(
                `This property offers excellent value at ${analysis.discountPercent}% below estimated market price of ${analysis.estimatedMarketPrice?.toLocaleString()}.`
            );
        }
        
        // Investment merit and deal quality
        if (analysis.dealQuality && analysis.score) {
            segments.push(
                `The deal represents ${analysis.dealQuality} investment quality with a score of ${analysis.score}/100, ` +
                `offering ${analysis.investmentAnalysis?.roi ? `an estimated ${analysis.investmentAnalysis.roi}% ROI` : 'strong return potential'}.`
            );
        }
        
        // Comparable analysis insight
        if (analysis.comparableInsights?.marketSegment) {
            segments.push(
                `Based on analysis of ${context.totalComparables} comparable sales, this ${analysis.comparableInsights.marketSegment} property ` +
                `is positioned as a ${analysis.comparableInsights.competitivePosition} offering in the current market.`
            );
        }
        
        // Risk and confidence assessment
        segments.push(
            `Analysis confidence is ${analysis.confidence}% based on ${analysis.detailedAnalysis ? 'comprehensive' : 'standard'} market comparison` +
            `${analysis.riskFactors && analysis.riskFactors.length > 0 ? ` with identified risks including ${analysis.riskFactors.slice(0, 2).join(' and ')}` : ''}.`
        );
        
        return segments.join(' ');
    }

    /**
     * Build comprehensive rentals reasoning from Claude's detailed analysis
     */
    buildComprehensiveRentalsReasoning(analysis, targetProperty, context) {
        const segments = [];
        
        // Market positioning
        if (analysis.percentBelowMarket && analysis.estimatedMarketRent) {
            const marketPosition = analysis.percentBelowMarket > 0 ? 'below' : 'above';
            segments.push(
                `This rental is priced ${Math.abs(analysis.percentBelowMarket)}% ${marketPosition} its estimated market value of ${analysis.estimatedMarketRent.toLocaleString()}/month, ` +
                `${analysis.keyMetrics?.rentPercentile ? `placing it in the ${100 - analysis.keyMetrics.rentPercentile}th percentile for value in ${targetProperty.neighborhood}` : 'representing significant market value'}.`
            );
        }
        
        // Rent stabilization assessment
        if (analysis.rentStabilizedProbability > 0) {
            segments.push(
                `The property has a ${analysis.rentStabilizedProbability}% probability of being rent-stabilized based on ` +
                `${analysis.rentStabilizedFactors?.length || 0} key indicators` +
                `${analysis.rentStabilizedFactors?.length > 0 ? ` including ${analysis.rentStabilizedFactors.slice(0, 2).join(' and ')}` : ''}.`
            );
        }
        
        // Investment merit
        if (analysis.investmentMerit && analysis.legalProtectionValue) {
            segments.push(
                `Investment merit is rated as ${analysis.investmentMerit} with ${analysis.legalProtectionValue.toLocaleString()} in estimated legal protection value, ` +
                `making it ${analysis.investmentMerit === 'exceptional' || analysis.investmentMerit === 'strong' ? 'an attractive' : 'a moderate'} opportunity.`
            );
        }
        
        // Confidence and analysis quality
        segments.push(
            `Analysis confidence is ${analysis.confidence || analysis.undervaluationConfidence}% based on comparison with ${context.totalComparables} market comparables` +
            `${analysis.detailedAnalysis ? ' using comprehensive market analysis' : ''}.`
        );
        
        return segments.join(' ');
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
     * Find potential rent-stabilized building matches
     */
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

    /**
     * Calculate market statistics for comparables
     */
    calculateMarketStatistics(comparables, type) {
        if (!comparables || comparables.length === 0) {
            return { priceStats: {}, psfStats: {} };
        }
        
        const priceField = type === 'sales' ? 'salePrice' : 'price';
        const prices = comparables.map(comp => comp[priceField] || comp.price || 0).filter(p => p > 0);
        const psfs = comparables.map(comp => comp.pricePerSqft || (comp.sqft > 0 ? (comp[priceField] || comp.price || 0) / comp.sqft : 0)).filter(p => p > 0);
        
        const calculateStats = (values) => {
            if (values.length === 0) return {};
            const sorted = values.slice().sort((a, b) => a - b);
            const sum = values.reduce((a, b) => a + b, 0);
            const mean = sum / values.length;
            const variance = values.reduce((acc, val) => acc + Math.pow(val - mean, 2), 0) / values.length;
            
            return {
                min: sorted[0],
                max: sorted[sorted.length - 1],
                median: sorted[Math.floor(sorted.length / 2)],
                mean: Math.round(mean),
                stdDev: Math.round(Math.sqrt(variance)),
                q1: sorted[Math.floor(sorted.length * 0.25)],
                q3: sorted[Math.floor(sorted.length * 0.75)]
            };
        };
        
        return {
            priceStats: calculateStats(prices),
            psfStats: calculateStats(psfs)
        };
    }

    /**
     * Analyze comparables and create similarity rankings
     */
    analyzeComparables(targetProperty, comparables, type) {
        const scoredComparables = comparables.map(comp => ({
            ...comp,
            similarity: this.calculatePropertySimilarity(targetProperty, comp, type)
        })).sort((a, b) => b.similarity - a.similarity);
        
        // Create analysis tiers
        const tiers = [
            {
                description: 'Exact matches (same bed/bath)',
                properties: scoredComparables.filter(c => c.bedrooms === targetProperty.bedrooms && c.bathrooms === targetProperty.bathrooms),
                count: 0,
                avgPrice: 0,
                avgPsf: 0
            },
            {
                description: 'Close matches (±1 bed/bath)',
                properties: scoredComparables.filter(c => 
                    Math.abs(c.bedrooms - targetProperty.bedrooms) <= 1 && 
                    Math.abs(c.bathrooms - targetProperty.bathrooms) <= 0.5
                ),
                count: 0,
                avgPrice: 0,
                avgPsf: 0
            },
            {
                description: 'Similar properties',
                properties: scoredComparables.filter(c => c.similarity >= 6),
                count: 0,
                avgPrice: 0,
                avgPsf: 0
            }
        ];
        
        // Calculate tier statistics
        tiers.forEach(tier => {
            tier.count = tier.properties.length;
            if (tier.count > 0) {
                const priceField = type === 'sales' ? 'salePrice' : 'price';
                const prices = tier.properties.map(p => p[priceField] || p.price || 0).filter(p => p > 0);
                const psfs = tier.properties.map(p => p.pricePerSqft || 0).filter(p => p > 0);
                
                tier.avgPrice = prices.length > 0 ? Math.round(prices.reduce((a, b) => a + b, 0) / prices.length) : 0;
                tier.avgPsf = psfs.length > 0 ? psfs.reduce((a, b) => a + b, 0) / psfs.length : 0;
            }
        });
        
        return {
            topComparables: scoredComparables.slice(0, 12),
            tiers: tiers.filter(t => t.count > 0)
        };
    }

    /**
     * Calculate property similarity score (0-10)
     */
    calculatePropertySimilarity(target, comparable, type) {
        let score = 0;
        
        // Bedroom/bathroom match (40% of score)
        if (target.bedrooms === comparable.bedrooms) score += 2;
        else if (Math.abs(target.bedrooms - comparable.bedrooms) === 1) score += 1;
        
        if (Math.abs(target.bathrooms - comparable.bathrooms) <= 0.5) score += 2;
        else if (Math.abs(target.bathrooms - comparable.bathrooms) <= 1) score += 1;
        
        // Square footage similarity (20% of score)
        if (target.sqft > 0 && comparable.sqft > 0) {
            const sqftDiff = Math.abs(target.sqft - comparable.sqft) / target.sqft;
            if (sqftDiff <= 0.1) score += 2;
            else if (sqftDiff <= 0.2) score += 1.5;
            else if (sqftDiff <= 0.3) score += 1;
        }
        
        // Building age similarity (10% of score)
        if (target.builtIn && comparable.builtIn) {
            const ageDiff = Math.abs(target.builtIn - comparable.builtIn);
            if (ageDiff <= 5) score += 1;
            else if (ageDiff <= 15) score += 0.5;
        }
        
        // Amenity overlap (20% of score)
        const targetAmenities = new Set(target.amenities || []);
        const comparableAmenities = new Set(comparable.amenities || []);
        const intersection = new Set([...targetAmenities].filter(x => comparableAmenities.has(x)));
        const union = new Set([...targetAmenities, ...comparableAmenities]);
        
        if (union.size > 0) {
            score += (intersection.size / union.size) * 2;
        }
        
        // Price range reasonableness (10% of score)
        const priceField = type === 'sales' ? 'salePrice' : 'price';
        const targetPrice = target[priceField] || target.price || 0;
        const compPrice = comparable[priceField] || comparable.price || 0;
        
        if (targetPrice > 0 && compPrice > 0) {
            const priceDiff = Math.abs(targetPrice - compPrice) / targetPrice;
            if (priceDiff <= 0.3) score += 1;
            else if (priceDiff <= 0.5) score += 0.5;
        }
        
        return Math.min(10, Math.max(0, score));
    }

    /**
     * Analyze neighborhood characteristics
     */
    analyzeNeighborhood(neighborhood, type) {
        // Default neighborhood analysis - can be enhanced with real data
        const neighborhoodData = {
            'soho': { tier: 'luxury', desirabilityScore: 9, pricePremium: '+40%', velocity: 'fast' },
            'tribeca': { tier: 'ultra-luxury', desirabilityScore: 10, pricePremium: '+60%', velocity: 'fast' },
            'west-village': { tier: 'luxury', desirabilityScore: 9, pricePremium: '+35%', velocity: 'moderate' },
            'east-village': { tier: 'mid-luxury', desirabilityScore: 8, pricePremium: '+20%', velocity: 'fast' },
            'lower-east-side': { tier: 'mid-market', desirabilityScore: 7, pricePremium: '+10%', velocity: 'fast' },
            'financial-district': { tier: 'mid-luxury', desirabilityScore: 7, pricePremium: '+15%', velocity: 'moderate' },
            'brooklyn-heights': { tier: 'luxury', desirabilityScore: 8, pricePremium: '+25%', velocity: 'moderate' },
            'dumbo': { tier: 'luxury', desirabilityScore: 8, pricePremium: '+25%', velocity: 'moderate' },
            'williamsburg': { tier: 'mid-luxury', desirabilityScore: 8, pricePremium: '+20%', velocity: 'fast' }
        };
        
        const data = neighborhoodData[neighborhood?.toLowerCase()] || {
            tier: 'mid-market',
            desirabilityScore: 6,
            pricePremium: '+5%',
            velocity: 'moderate'
        };
        
        if (type === 'sales') {
            return {
                ...data,
                investmentOutlook: data.desirabilityScore >= 8 ? 'strong' : 'moderate',
                marketAppreciation: data.tier === 'luxury' || data.tier === 'ultra-luxury' ? 'strong' : 'moderate'
            };
        } else {
            return {
                ...data,
                tenantProfile: data.tier === 'luxury' ? 'high-income professionals' : 'young professionals',
                rentalOutlook: data.desirabilityScore >= 8 ? 'strong demand' : 'steady demand',
                rentPremium: data.pricePremium
            };
        }
    }

    /**
     * Validation functions
     */
    validateEnhancedSalesAnalysis(analysis) {
        return analysis && 
               typeof analysis.estimatedMarketPrice === 'number' &&
               typeof analysis.discountPercent === 'number' &&
               typeof analysis.confidence === 'number' &&
               analysis.confidence >= 0 && analysis.confidence <= 100;
    }

    validateEnhancedRentalsAnalysis(analysis) {
        return analysis && 
               (typeof analysis.estimatedMarketRent === 'number' || typeof analysis.estimatedMarketPrice === 'number') &&
               typeof analysis.percentBelowMarket === 'number' &&
               typeof analysis.confidence === 'number' &&
               analysis.confidence >= 0 && analysis.confidence <= 100 &&
               typeof analysis.rentStabilizedProbability === 'number';
    }

    /**
     * Helper functions
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

    calculateAmenityScore(amenities) {
        const standardAmenities = [
            'doorman', 'elevator', 'laundry', 'gym', 'rooftop', 'parking',
            'dishwasher', 'air-conditioning', 'hardwood-floors', 'balcony'
        ];
        
        let score = 0;
        for (const amenity of amenities) {
            const normalizedAmenity = amenity.toLowerCase();
            if (standardAmenities.some(std => normalizedAmenity.includes(std))) {
                score += 10;
            }
        }
        
        return Math.min(100, score);
    }

    assessConditionFromDescription(description) {
        const desc = description.toLowerCase();
        let score = 50; // Base score
        
        // Positive indicators
        if (desc.includes('renovated') || desc.includes('updated')) score += 20;
        if (desc.includes('new') || desc.includes('modern')) score += 15;
        if (desc.includes('luxury') || desc.includes('high-end')) score += 15;
        if (desc.includes('pristine') || desc.includes('immaculate')) score += 10;
        
        // Negative indicators
        if (desc.includes('needs work') || desc.includes('fixer')) score -= 30;
        if (desc.includes('original') && !desc.includes('restored')) score -= 10;
        
        return Math.min(100, Math.max(0, score));
    }

    assessBuildingAge(builtIn) {
        if (!builtIn) return 'unknown';
        if (builtIn < 1974) return 'high_stabilization_potential';
        if (builtIn < 1985) return 'moderate_stabilization_potential';
        return 'low_stabilization_potential';
    }

    assessUnitCount(property) {
        // This would need building data - placeholder logic
        return 'unknown_unit_count';
    }

    assessRentLevel(property) {
        // Based on price per sqft relative to market
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
        if (desc.includes('no broker fee') && property.noFee) {
            indicators.push('no_fee_indicator');
        }
        
        return indicators;
    }

    delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

module.exports = EnhancedClaudeMarketAnalyzer;
