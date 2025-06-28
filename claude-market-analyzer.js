// claude-market-analyzer.js
// ENHANCED CLAUDE-POWERED MARKET ANALYSIS ENGINE
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
            console.log(`   🎯 Investment grade: ${analysis.grade}`);
            
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
                grade: analysis.grade,
                // Enhanced metrics
                detailedAnalysis: analysis.detailedAnalysis || {},
                keyMetrics: analysis.keyMetrics || {},
                comparableInsights: analysis.comparableInsights || {},
                investmentRating: analysis.investmentRating || 'B',
                marketPosition: analysis.marketPosition || 'mid-market',
                riskFactors: analysis.riskFactors || [],
                roi: ((analysis.estimatedMarketPrice - targetProperty.salePrice) / targetProperty.salePrice * 100)
            };
            
        } catch (error) {
            console.warn(`   ⚠️ Enhanced Claude sales analysis error: ${error.message}`);
            return {
                isUndervalued: false,
                discountPercent: 0,
                estimatedMarketPrice: targetProperty.salePrice,
                actualPrice: targetProperty.salePrice,
                confidence: 0,
                method: 'enhanced_claude_error',
                reasoning: `Enhanced analysis error: ${error.message}`
            };
        }
    }

    /**
     * ENHANCED RENTALS ANALYSIS with comprehensive reasoning and detailed insights
     */
    async analyzeRentalsUndervaluation(targetProperty, comparableProperties, rentStabilizedBuildings, neighborhood, options = {}) {
        const threshold = options.undervaluationThreshold || 15;
        
        console.log(`🤖 Enhanced Claude analyzing rental: ${targetProperty.address}`);
        
        try {
            // Build enhanced rental market context
            const enhancedContext = this.buildEnhancedRentalsContext(
                targetProperty, 
                comparableProperties, 
                rentStabilizedBuildings, 
                neighborhood
            );
            
            // Get Claude's sophisticated rental analysis
            const claudeResponse = await this.callClaudeForEnhancedRentalsAnalysis(enhancedContext, threshold);
            
            if (!claudeResponse.success) {
                return {
                    success: false,
                    error: claudeResponse.error || 'Enhanced analysis failed',
                    estimatedMarketRent: targetProperty.price,
                    percentBelowMarket: 0,
                    confidence: 0,
                    method: 'enhanced_claude_analysis_failed'
                };
            }
            
            const analysis = claudeResponse.analysis;
            
            // Validate enhanced response
            if (!this.validateEnhancedRentalsAnalysis(analysis)) {
                throw new Error('Invalid enhanced rental analysis structure from Claude');
            }
            
            // Build comprehensive reasoning
            const comprehensiveReasoning = this.buildComprehensiveRentalsReasoning(analysis, targetProperty, enhancedContext);
            
            console.log(`   💰 Claude enhanced market rent: $${analysis.estimatedMarketRent.toLocaleString()}`);
            console.log(`   📊 Below market: ${analysis.percentBelowMarket.toFixed(1)}%`);
            console.log(`   ✅ Confidence: ${analysis.confidence}%`);
            console.log(`   🏠 Rent stabilized probability: ${analysis.rentStabilizedProbability}%`);
            console.log(`   🎯 Investment grade: ${analysis.grade || 'B'}`);
            
            return {
                success: true,
                estimatedMarketRent: analysis.estimatedMarketRent,
                percentBelowMarket: analysis.percentBelowMarket,
                confidence: analysis.confidence,
                method: 'enhanced_claude_comparative_analysis',
                comparablesUsed: comparableProperties.length,
                adjustments: analysis.adjustmentBreakdown || [],
                calculationSteps: analysis.calculationSteps || [],
                baseMarketRent: analysis.baseMarketRent,
                totalAdjustments: analysis.totalAdjustments || 0,
                confidenceFactors: analysis.confidenceFactors || {},
                reasoning: comprehensiveReasoning,
                // Enhanced rent stabilization analysis
                rentStabilizedProbability: analysis.rentStabilizedProbability,
                rentStabilizedFactors: analysis.rentStabilizedFactors || [],
                rentStabilizedExplanation: analysis.rentStabilizedExplanation,
                // Enhanced metrics
                detailedAnalysis: analysis.detailedAnalysis || {},
                keyMetrics: analysis.keyMetrics || {},
                comparableInsights: analysis.comparableInsights || {},
                investmentRating: analysis.investmentRating || 'B',
                marketPosition: analysis.marketPosition || 'mid-market',
                riskFactors: analysis.riskFactors || [],
                annualSavings: (analysis.estimatedMarketRent - targetProperty.price) * 12,
                effectiveValue: analysis.effectiveValue || analysis.estimatedMarketRent
            };
            
        } catch (error) {
            console.warn(`   ⚠️ Enhanced Claude rental analysis error: ${error.message}`);
            return {
                success: false,
                error: `Enhanced analysis error: ${error.message}`,
                estimatedMarketRent: targetProperty.price,
                percentBelowMarket: 0,
                confidence: 0,
                method: 'enhanced_claude_error'
            };
        }
    }

    /**
     * Build enhanced market context for sales analysis with comprehensive data
     */
    buildEnhancedSalesContext(targetProperty, comparables, neighborhood) {
        // Calculate detailed market statistics
        const marketStats = this.calculateEnhancedMarketStats(comparables, 'sales');
        
        // Analyze property details comprehensively
        const propertyAnalysis = this.analyzePropertyDetails(targetProperty, 'sales');
        
        // Get neighborhood context and insights
        const neighborhoodContext = this.getNeighborhoodContext(neighborhood);
        
        // Analyze comparables by tiers and similarity
        const comparableAnalysis = this.analyzeComparablesByTiers(comparables, targetProperty, 'sales');
        
        // Filter and enhance comparables
        const enhancedComparables = comparables
            .filter(comp => comp.salePrice > 0 && comp.bedrooms !== undefined)
            .slice(0, 20) // Increased for better analysis
            .map(comp => ({
                address: comp.address,
                price: comp.salePrice,
                bedrooms: comp.bedrooms,
                bathrooms: comp.bathrooms,
                sqft: comp.sqft || null,
                pricePerSqft: comp.sqft > 0 ? comp.salePrice / comp.sqft : null,
                builtIn: comp.builtIn || null,
                amenities: (comp.amenities || []).slice(0, 15),
                description: (comp.description || '').substring(0, 400) + '...',
                daysOnMarket: comp.daysOnMarket || null,
                similarity: this.calculateSimilarityScore(comp, targetProperty)
            }))
            .sort((a, b) => b.similarity - a.similarity);

        return {
            targetProperty: {
                address: targetProperty.address,
                price: targetProperty.salePrice,
                bedrooms: targetProperty.bedrooms,
                bathrooms: targetProperty.bathrooms,
                sqft: targetProperty.sqft || null,
                pricePerSqft: targetProperty.sqft > 0 ? targetProperty.salePrice / targetProperty.sqft : null,
                builtIn: targetProperty.builtIn || null,
                amenities: targetProperty.amenities || [],
                description: (targetProperty.description || '').substring(0, 600) + '...',
                neighborhood: neighborhood,
                borough: targetProperty.borough,
                analysis: propertyAnalysis
            },
            comparables: enhancedComparables,
            marketStats: marketStats,
            neighborhood: neighborhoodContext,
            comparableAnalysis: comparableAnalysis,
            totalComparables: comparables.length
        };
    }

    /**
     * Build enhanced market context for rentals analysis with comprehensive data
     */
    buildEnhancedRentalsContext(targetProperty, comparables, rentStabilizedBuildings, neighborhood) {
        // Calculate detailed market statistics
        const marketStats = this.calculateEnhancedMarketStats(comparables, 'rentals');
        
        // Analyze property details comprehensively
        const propertyAnalysis = this.analyzePropertyDetails(targetProperty, 'rentals');
        
        // Get neighborhood context and insights
        const neighborhoodContext = this.getNeighborhoodContext(neighborhood);
        
        // Analyze comparables by tiers and similarity
        const comparableAnalysis = this.analyzeComparablesByTiers(comparables, targetProperty, 'rentals');
        
        // Enhanced rent stabilization analysis
        const rentStabilizationContext = this.buildEnhancedRentStabilizationContext(
            targetProperty, 
            rentStabilizedBuildings, 
            neighborhood
        );

        // Filter and enhance comparables
        const enhancedComparables = comparables
            .filter(comp => comp.price > 0 && comp.bedrooms !== undefined)
            .slice(0, 20)
            .map(comp => ({
                address: comp.address,
                price: comp.price,
                bedrooms: comp.bedrooms,
                bathrooms: comp.bathrooms,
                sqft: comp.sqft || null,
                pricePerSqft: comp.sqft > 0 ? comp.price / comp.sqft : null,
                amenities: (comp.amenities || []).slice(0, 15),
                noFee: comp.noFee || false,
                description: (comp.description || '').substring(0, 400) + '...',
                similarity: this.calculateSimilarityScore(comp, targetProperty)
            }))
            .sort((a, b) => b.similarity - a.similarity);

        return {
            targetProperty: {
                address: targetProperty.address,
                price: targetProperty.price,
                bedrooms: targetProperty.bedrooms,
                bathrooms: targetProperty.bathrooms,
                sqft: targetProperty.sqft || null,
                pricePerSqft: targetProperty.sqft > 0 ? targetProperty.price / targetProperty.sqft : null,
                builtIn: targetProperty.builtIn || null,
                amenities: targetProperty.amenities || [],
                description: (targetProperty.description || '').substring(0, 600) + '...',
                neighborhood: neighborhood,
                borough: targetProperty.borough,
                noFee: targetProperty.noFee || false,
                analysis: propertyAnalysis
            },
            comparables: enhancedComparables,
            marketStats: marketStats,
            neighborhood: neighborhoodContext,
            comparableAnalysis: comparableAnalysis,
            rentStabilizationContext: rentStabilizationContext,
            totalComparables: comparables.length
        };
    }

    /**
     * Calculate enhanced market statistics with comprehensive metrics
     */
    calculateEnhancedMarketStats(comparables, type) {
        const prices = comparables.map(c => type === 'sales' ? c.salePrice : c.price).filter(p => p > 0);
        const psfs = comparables
            .filter(c => c.sqft > 0)
            .map(c => (type === 'sales' ? c.salePrice : c.price) / c.sqft)
            .filter(p => p > 0 && isFinite(p));
        const doms = comparables.map(c => c.daysOnMarket || 0).filter(d => d >= 0);
        
        const sortedPrices = [...prices].sort((a, b) => a - b);
        const sortedPsfs = [...psfs].sort((a, b) => a - b);
        
        return {
            priceStats: {
                median: this.calculateMedian(prices),
                mean: prices.reduce((a, b) => a + b, 0) / prices.length,
                min: Math.min(...prices),
                max: Math.max(...prices),
                stdDev: this.calculateStandardDeviation(prices),
                q1: this.calculatePercentile(sortedPrices, 25),
                q3: this.calculatePercentile(sortedPrices, 75),
                range: Math.max(...prices) - Math.min(...prices)
            },
            psfStats: {
                median: this.calculateMedian(psfs),
                mean: psfs.reduce((a, b) => a + b, 0) / psfs.length,
                min: Math.min(...psfs),
                max: Math.max(...psfs),
                stdDev: this.calculateStandardDeviation(psfs),
                q1: this.calculatePercentile(sortedPsfs, 25),
                q3: this.calculatePercentile(sortedPsfs, 75)
            },
            marketVelocity: {
                avgDaysOnMarket: doms.length > 0 ? doms.reduce((a, b) => a + b, 0) / doms.length : null,
                fastSales: doms.filter(d => d <= 30).length,
                slowSales: doms.filter(d => d > 90).length,
                medianDOM: this.calculateMedian(doms)
            },
            bedBathDistribution: this.analyzeBedBathDistribution(comparables),
            amenityFrequency: this.analyzeAmenityFrequency(comparables),
            priceDistribution: this.analyzePriceDistribution(prices),
            dataQuality: {
                totalSamples: comparables.length,
                withSqft: comparables.filter(c => c.sqft > 0).length,
                withAmenities: comparables.filter(c => (c.amenities || []).length > 0).length,
                completeness: this.calculateDataCompleteness(comparables)
            }
        };
    }

    /**
     * Analyze property details comprehensively
     */
    analyzePropertyDetails(property, type) {
        const sqft = property.sqft || 0;
        const price = type === 'sales' ? property.salePrice : property.price;
        
        return {
            pricePerSqft: sqft > 0 ? price / sqft : null,
            spaceEfficiency: this.calculateSpaceEfficiency(property),
            amenityScore: this.calculateAmenityScore(property.amenities || []),
            amenityCount: (property.amenities || []).length,
            descriptionAnalysis: this.analyzeDescription(property.description || ''),
            buildingQuality: this.assessBuildingQuality(property),
            locationFactors: this.analyzeLocationFactors(property),
            marketPosition: this.assessMarketPosition(property, type),
            conditionScore: this.assessConditionFromDescription(property.description || ''),
            uniqueFeatures: this.extractUniqueFeatures(property.description || '', property.amenities || [])
        };
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
     * Core Claude API call with enhanced error handling and retry logic
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
                        max_tokens: 4000, // Increased for detailed analysis
                        temperature: 0.05, // Very low for consistent analysis
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
                        timeout: 45000 // Increased timeout for complex analysis
                    }
                );
                
                const responseText = response.data.content[0].text;
                console.log(`   ✅ Enhanced Claude response received (${responseText.length} chars)`);
                
                // Parse JSON response with enhanced error handling
                try {
                    const analysis = JSON.parse(responseText);
                    return { success: true, analysis };
                } catch (parseError) {
                    console.warn(`   ⚠️ JSON parse error, attempting enhanced extraction: ${parseError.message}`);
                    
                    // Enhanced JSON extraction
                    const jsonMatch = responseText.match(/\{[\s\S]*\}/);
                    if (jsonMatch) {
                        try {
                            const analysis = JSON.parse(jsonMatch[0]);
                            return { success: true, analysis };
                        } catch (secondParseError) {
                            console.warn(`   ⚠️ Second parse attempt failed: ${secondParseError.message}`);
                        }
                    }
                    
                    // Try to extract key-value pairs if JSON fails
                    const extractedData = this.extractDataFromResponse(responseText);
                    if (extractedData) {
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
6. Neighborhood-specific premium/discount analysis

NYC NEIGHBORHOOD MARKET TIERS (affects base pricing):
TIER 1 (Ultra-Premium +35-50%): SoHo, Tribeca, West Village, Greenwich Village
TIER 2 (Premium +25-35%): NoLita, Chelsea, Gramercy, Dumbo, Brooklyn Heights
TIER 3 (High-End +15-25%): Upper East Side, Upper West Side, Park Slope, Williamsburg
TIER 4 (Desirable +10-20%): East Village, Lower East Side, Long Island City, Fort Greene
TIER 5 (Emerging +5-15%): Crown Heights, Prospect Heights, Astoria, Bed-Stuy
TIER 6 (Value 0-10%): Bushwick, Ridgewood, Mott Haven, Concourse

ENHANCED BUILDING ANALYSIS:
PRE-WAR LUXURY (pre-1940):
- Manhattan: +$200-500k (architectural details, high ceilings, solid construction)
- Brooklyn: +$100-300k (brownstone/limestone premium, character)
- Queens/Bronx: +$50-150k (solid construction, architectural interest)

POST-WAR CLASSIC (1940-1980):
- Manhattan: Baseline to +$100k (solid construction, less character)
- Brooklyn: -$50k to +$50k (functional but less desirable than pre-war)
- Queens/Bronx: Baseline (standard construction for area)

MODERN LUXURY (1980-2010):
- Manhattan: +$100-300k (modern systems, better layouts)
- Brooklyn: +$50-200k (contemporary amenities, efficiency)
- Queens/Bronx: +$25-100k (modern conveniences)

NEW DEVELOPMENT (post-2010):
- Manhattan: +$150-400k (luxury amenities, warranties, smart systems)
- Brooklyn: +$75-250k (modern design, energy efficiency, amenities)
- Queens/Bronx: +$50-150k (contemporary features, efficiency)

SOPHISTICATED AMENITY VALUATION:
MANHATTAN LUXURY AMENITIES:
- Full-time doorman: +$250-500k (essential luxury service)
- Concierge services: +$100-200k (premium convenience)
- Fitness center/gym: +$100-250k (valuable space-saving amenity)
- Roof deck/terrace: +$150-400k (rare outdoor space premium)
- Swimming pool: +$200-500k (ultra-luxury amenity)
- Parking (deeded): +$200-400k (extremely valuable in Manhattan)
- In-unit laundry: +$75-150k (major convenience)
- Central air: +$50-100k (comfort factor)
- Private outdoor space: +$200-600k (balcony/terrace premium)
- Smart home features: +$25-75k (modern convenience)

BROOKLYN PREMIUM AMENITIES:
- Full-time doorman: +$125-250k (luxury in outer borough)
- Fitness center: +$75-150k (valuable amenity)
- Roof deck: +$100-200k (outdoor space value)
- Parking: +$100-200k (valuable but more available)
- In-unit laundry: +$50-100k (convenience factor)
- Private outdoor space: +$100-300k (outdoor space premium)

CONDITION AND QUALITY PREMIUMS:
- "Gut renovated" with high-end finishes: +20-30% premium
- "Recently renovated" with quality materials: +10-20% premium
- "Move-in ready" condition: +5-10% premium
- "Mint condition" luxury finishes: +15-25% premium
- "Original condition" in good building: Baseline
- "Needs updating" or "TLC": -10-20% discount
- "Fixer-upper" or major renovation needed: -20-35% discount

MARKET TIMING CONSIDERATIONS:
- Spring market (Mar-May): +2-5% premium (peak activity)
- Summer market (Jun-Aug): Baseline to +2%
- Fall market (Sep-Nov): +3-7% premium (peak luxury market)
- Winter market (Dec-Feb): -5-10% discount (slower market)

ENHANCED VALUATION METHODOLOGY:
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
  "confidence": number (0-100),
  "grade": "A+" to "C-",
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
    "pricePercentile": number (0-100),
    "amenityScore": number (0-100),
    "marketVelocityScore": number (0-100),
    "neighborhoodDesirability": number (1-10),
    "buildingQuality": number (1-10),
    "investmentGrade": "A+", "A", "B+", "B", "C+", "C", "D"
  },
  "comparableInsights": {
    "bestComparables": ["top 3 most similar addresses"],
    "priceRange": "price range of most similar properties",
    "marketSegment": "ultra-luxury|luxury|mid-luxury|mid-market|value",
    "competitivePosition": "premium|competitive|value|discount"
  },
  "investmentAnalysis": {
    "roi": number (potential return percentage),
    "paybackPeriod": number (years),
    "marketAppreciation": "strong|moderate|weak",
    "liquidityScore": number (1-10),
    "cashFlow": "positive|neutral|negative"
  },
  "riskFactors": ["specific risk factor 1", "risk factor 2"],
  "marketPosition": "ultra-luxury|luxury|mid-market|value|distressed"
}

Be exceptionally detailed, specific, and data-driven. Include exact figures, percentiles, and actionable insights.`;
    }

    /**
     * ENHANCED SYSTEM PROMPT FOR RENTALS ANALYSIS with sophisticated reasoning
     */
    buildEnhancedRentalsSystemPrompt() {
        return `You are an expert NYC rental market analyst and rent stabilization authority with deep knowledge of micro-market pricing, legal frameworks, and investment analysis. Your analysis must provide exceptional detail on market positioning, rent stabilization probability, and investment value.

ENHANCED RENTAL ANALYSIS FRAMEWORK:
1. Comprehensive market rent assessment with percentile rankings
2. Detailed rent stabilization legal analysis with confidence scoring
3. Investment merit evaluation for rental properties
4. Risk assessment and tenant protection analysis
5. Market timing and seasonal considerations
6. Neighborhood-specific rent premium analysis

NYC NEIGHBORHOOD RENTAL MARKET TIERS:
TIER 1 (Ultra-Premium +40-60%): SoHo, Tribeca, West Village, Greenwich Village
TIER 2 (Premium +30-40%): NoLita, Chelsea, Gramercy, Dumbo, Brooklyn Heights
TIER 3 (High-End +20-30%): Upper East Side, Upper West Side, Park Slope, Williamsburg
TIER 4 (Desirable +15-25%): East Village, Lower East Side, Long Island City, Fort Greene
TIER 5 (Emerging +10-20%): Crown Heights, Prospect Heights, Astoria, Bed-Stuy
TIER 6 (Value +5-15%): Bushwick, Ridgewood, Mott Haven, Concourse

RENT STABILIZATION LEGAL FRAMEWORK:
DEFINITIVE INDICATORS (95-100% confidence):
- Explicit "rent stabilized" mention in listing
- "Preferential rent" language (legal term for below-maximum stabilized rent)
- "DHCR guidelines" or "Rent Guidelines Board" references
- "Legal regulated rent" or "maximum legal rent" mentions
- Building in official DHCR stabilized database

STRONG LEGAL INDICATORS (85-95% confidence):
- Building constructed pre-1974 + 6+ units (automatic RSC coverage)
- Building 1974-2019 + 6+ units + rent 30%+ below market (likely stabilized)
- "No rent increases above guidelines" language
- "Lease renewal follows RGB guidelines"
- Historical rent stabilization registration records

CIRCUMSTANTIAL INDICATORS (60-80% confidence):
- No broker fee + rent 25%+ below market + 6+ unit building
- Significantly below-market rent in older building (6+ units)
- Long-term lease availability (2+ years) at below-market rates
- Building characteristics matching stabilized profile
- Neighborhood with high stabilization rates

BUILDING TYPE RENT PREMIUMS:
PRE-WAR LUXURY (pre-1940):
- Manhattan: +$300-800/month (architectural character, high ceilings)
- Brooklyn: +$200-500/month (brownstone charm, period details)
- Queens/Bronx: +$100-300/month (solid construction, character)

POST-WAR CLASSIC (1940-1980):
- Manhattan: Baseline to +$200/month (solid but less character)
- Brooklyn: -$100 to +$100/month (functional construction)
- Queens/Bronx: Baseline (standard for area)

MODERN BUILDINGS (1980-2010):
- Manhattan: +$200-500/month (modern systems, layouts)
- Brooklyn: +$100-300/month (contemporary amenities)
- Queens/Bronx: +$50-200/month (modern conveniences)

NEW LUXURY (post-2010):
- Manhattan: +$400-1000/month (luxury amenities, smart features)
- Brooklyn: +$200-600/month (modern design, efficiency)
- Queens/Bronx: +$100-400/month (contemporary features)

SOPHISTICATED AMENITY RENT ADJUSTMENTS:
MANHATTAN LUXURY AMENITIES:
- Full-time doorman: +$400-800/month (essential luxury service)
- Concierge services: +$200-400/month (premium convenience)
- Fitness center: +$150-350/month (valuable space-saving amenity)
- Roof deck/pool: +$200-500/month (rare outdoor space)
- In-unit laundry: +$150-300/month (major convenience)
- Dishwasher: +$75-150/month (kitchen convenience)
- Central air: +$100-200/month (comfort premium)
- Parking (deeded): +$300-600/month (extremely valuable)
- Balcony/terrace: +$300-800/month (outdoor space premium)
- Gym/pool in building: +$200-400/month (luxury amenity)

BROOKLYN PREMIUM AMENITIES:
- Full-time doorman: +$200-400/month (luxury in outer borough)
- Fitness center: +$100-200/month (valuable amenity)
- Roof deck: +$150-300/month (outdoor space value)
- In-unit laundry: +$100-200/month (major convenience)
- Parking: +$150-300/month (valuable but more available)
- Balcony/terrace: +$150-400/month (outdoor space)

QUEENS/BRONX AMENITIES:
- Doorman: +$100-250/month (luxury amenity)
- Fitness center: +$75-150/month (valuable feature)
- Parking: +$100-200/month (expected but valuable)
- In-unit laundry: +$75-150/month (convenience)

BROKER FEE IMPACT ON EFFECTIVE VALUE:
- No Fee: Equivalent to 1-2 months rent savings (15-30% value increase)
- 1 Month Fee: Standard market (baseline)
- 2+ Month Fee: Above market (reduces effective value by 10-20%)

SEASONAL RENT TIMING:
- September peak: +8-15% above baseline (college/corporate moves)
- October-November: +5-10% above baseline (peak activity)
- December-February: -10-15% below baseline (winter valley)
- March-May: -5-10% below baseline (pre-peak season)
- June-August: Baseline to +5% (summer activity)

ENHANCED RENT STABILIZATION DETECTION:
LEGAL ANALYSIS FACTORS:
1. Building construction date and unit count (automatic qualification)
2. Rent level vs. market rate (stabilized units often below market)
3. Lease language and renewal terms (guidelines-based increases)
4. Building registration status in DHCR database
5. Historical rent progression (consistent with guidelines)
6. Landlord behavior patterns (renewals, increases)

MARKET VALUATION METHODOLOGY:
1. Identify non-stabilized comparables with similar characteristics
2. Weight comparables by similarity and reliability
3. Apply neighborhood tier premium to establish market baseline
4. Adjust for building type and construction era
5. Add amenity premiums using borough-specific values
6. Factor broker fee situation into effective value
7. Apply seasonal timing adjustments
8. Calculate stabilization probability and protection value
9. Assess investment merit and cash flow potential
10. Provide comprehensive risk and opportunity analysis

REQUIRED RESPONSE FORMAT (JSON only):
{
  "estimatedMarketRent": number,
  "percentBelowMarket": number,
  "confidence": number (0-100),
  "grade": "A+" to "C-",
  "baseMarketRent": number,
  "totalAdjustments": number,
  "adjustmentBreakdown": [
    {"factor": "neighborhood_tier", "adjustment": number, "reasoning": "detailed explanation"},
    {"factor": "building_type", "adjustment": number, "reasoning": "detailed explanation"},
    {"factor": "amenities", "adjustment": number, "reasoning": "detailed explanation"},
    {"factor": "broker_fee", "adjustment": number, "reasoning": "detailed explanation"},
    {"factor": "seasonal_timing", "adjustment": number, "reasoning": "detailed explanation"}
  ],
  "reasoning": "Comprehensive 3-4 sentence analysis with specific data points",
  "detailedAnalysis": {
    "rentAnalysis": "Detailed rent positioning with percentiles and market context",
    "amenityAnalysis": "Comprehensive amenity value assessment",
    "stabilizationAnalysis": "Detailed rent stabilization legal and circumstantial analysis",
    "investmentMerit": "Investment opportunity assessment with cash flow analysis",
    "riskFactors": "Specific risks and tenant protection considerations"
  },
  "keyMetrics": {
    "rentPercentile": number (0-100),
    "amenityScore": number (0-100),
    "marketVelocityScore": number (0-100),
    "neighborhoodDesirability": number (1-10),
    "buildingQuality": number (1-10),
    "stabilizationConfidence": number (0-100)
  },
  "rentStabilizedProbability": number (0-100),
  "rentStabilizedFactors": ["specific legal indicator 1", "indicator 2"],
  "rentStabilizedExplanation": "Comprehensive legal and circumstantial analysis",
  "comparableInsights": {
    "bestComparables": ["top 3 most similar addresses"],
    "rentRange": "rent range of most similar properties",
    "marketSegment": "ultra-luxury|luxury|mid-luxury|mid-market|value",
    "competitivePosition": "premium|competitive|value|discount"
  },
  "investmentAnalysis": {
    "annualSavings": number (below-market savings per year),
    "effectiveValue": number (including broker fee consideration),
    "stabilizationValue": number (value of rent protection),
    "liquidityScore": number (1-10),
    "tenantProtection": "strong|moderate|weak"
  },
  "confidenceFactors": {
    "data_quality": number (0-100),
    "comparable_match": number (0-100),
    "stabilization_indicators": number (0-100),
    "legal_framework": number (0-100)
  },
  "riskFactors": ["specific risk factor 1", "risk factor 2"],
  "marketPosition": "ultra-luxury|luxury|mid-market|value|stabilized",
  "effectiveValue": number,
  "marketTiming": "peak|high|baseline|low|valley"
}

Be exceptionally detailed and legally precise. Include specific data points, percentiles, and actionable investment insights.`;
    }

    /**
     * Build enhanced user prompt for sales analysis
     */
    buildEnhancedSalesUserPrompt(enhancedContext, threshold) {
        const target = enhancedContext.targetProperty;
        const market = enhancedContext.marketStats;
        const neighborhood = enhancedContext.neighborhood;
        
        return `Analyze this NYC property for sophisticated investment opportunity assessment:

TARGET PROPERTY DETAILS:
Address: ${target.address}
Current Price: ${target.price.toLocaleString()}
Price/sqft: ${target.pricePerSqft?.toFixed(0) || 'N/A'}
Layout: ${target.bedrooms}BR/${target.bathrooms}BA
Square Feet: ${target.sqft || 'Not listed'}
Built: ${target.builtIn || 'Unknown'}
Neighborhood: ${target.neighborhood} (${target.borough})
Amenities: ${target.amenities.join(', ') || 'None listed'}
Amenity Score: ${target.analysis.amenityScore}/100
Condition Score: ${target.analysis.conditionScore}/100
Description: ${target.description}

ENHANCED MARKET CONTEXT:
Total Comparables: ${enhancedContext.totalComparables}
Median Price: ${market.priceStats.median?.toLocaleString()}
Mean Price: ${market.priceStats.mean?.toLocaleString()}
Price Range: ${market.priceStats.min?.toLocaleString()} - ${market.priceStats.max?.toLocaleString()}
Standard Deviation: ${market.priceStats.stdDev?.toLocaleString()}
Q1 (25th percentile): ${market.priceStats.q1?.toLocaleString()}
Q3 (75th percentile): ${market.priceStats.q3?.toLocaleString()}

PRICE PER SQFT ANALYSIS:
Median PSF: ${market.psfStats.median?.toFixed(0)}
Mean PSF: ${market.psfStats.mean?.toFixed(0)}
PSF Range: ${market.psfStats.min?.toFixed(0)} - ${market.psfStats.max?.toFixed(0)}
PSF Std Dev: ${market.psfStats.stdDev?.toFixed(0)}

MARKET VELOCITY INDICATORS:
Avg Days on Market: ${market.marketVelocity.avgDaysOnMarket?.toFixed(0) || 'N/A'} days
Fast Sales (<30 days): ${market.marketVelocity.fastSales}/${enhancedContext.totalComparables}
Slow Sales (>90 days): ${market.marketVelocity.slowSales}/${enhancedContext.totalComparables}
Median DOM: ${market.marketVelocity.medianDOM?.toFixed(0) || 'N/A'} days

TOP COMPARABLE PROPERTIES (by similarity):
${enhancedContext.comparables.slice(0, 8).map((comp, i) => 
  `${i+1}. ${comp.address} - ${comp.price.toLocaleString()} | ${comp.bedrooms}BR/${comp.bathrooms}BA | ${comp.sqft || 'N/A'} sqft | ${comp.pricePerSqft?.toFixed(0) || 'N/A'}/sqft | Built: ${comp.builtIn || 'N/A'} | DOM: ${comp.daysOnMarket || 'N/A'} | Similarity: ${comp.similarity?.toFixed(1) || 'N/A'}/10 | Amenities: ${comp.amenities.slice(0, 5).join(', ')}`
).join('\n')}

NEIGHBORHOOD ANALYSIS:
Market Tier: ${neighborhood.tier}
Desirability Score: ${neighborhood.desirabilityScore}/10
Price Premium: ${neighborhood.pricePremium}
Typical Buyer Profile: ${neighborhood.buyerProfile}
Market Velocity: ${neighborhood.velocity}
Investment Outlook: ${neighborhood.investmentOutlook}

BED/BATH DISTRIBUTION IN MARKET:
${Object.entries(market.bedBathDistribution).map(([key, count]) => 
  `${key}: ${count} properties`
).join(', ')}

AMENITY FREQUENCY ANALYSIS:
${Object.entries(market.amenityFrequency).slice(0, 10).map(([amenity, count]) => 
  `${amenity}: ${count} properties (${((count/enhancedContext.totalComparables)*100).toFixed(1)}%)`
).join(', ')}

DATA QUALITY METRICS:
Total Samples: ${market.dataQuality.totalSamples}
With Square Footage: ${market.dataQuality.withSqft} (${((market.dataQuality.withSqft/market.dataQuality.totalSamples)*100).toFixed(1)}%)
With Amenities: ${market.dataQuality.withAmenities} (${((market.dataQuality.withAmenities/market.dataQuality.totalSamples)*100).toFixed(1)}%)
Data Completeness: ${market.dataQuality.completeness.toFixed(1)}%

COMPARABLE ANALYSIS TIERS:
${enhancedContext.comparableAnalysis.tiers.map((tier, i) => 
  `Tier ${i+1} (${tier.description}): ${tier.count} properties, Avg: ${tier.avgPrice?.toLocaleString()}, Avg PSF: ${tier.avgPsf?.toFixed(0)}`
).join('\n')}

ANALYSIS REQUIREMENTS:
- Determine if property is ${threshold}%+ below sophisticated market valuation
- Provide percentile ranking within neighborhood and building type
- Calculate investment-grade metrics (ROI, payback period, market position)
- Assess competitive positioning and market timing
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
  `${i+1}. ${comp.address} - ${comp.price.toLocaleString()}/month | ${comp.bedrooms}BR/${comp.bathrooms}BA | ${comp.sqft || 'N/A'} sqft | ${comp.pricePerSqft?.toFixed(2) || 'N/A'}/sqft | No Fee: ${comp.noFee ? 'YES' : 'NO'} | Similarity: ${comp.similarity?.toFixed(1) || 'N/A'}/10 | Amenities: ${comp.amenities.slice(0, 5).join(', ')}`
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
  `${rsContext.strongestMatch.address} (Confidence: ${rsContext.matchConfidence}%)` : 'None'}
Registration Status: ${rsContext.registrationStatus}
Legal Indicators: ${rsContext.legalIndicators.length} found
Stabilization Probability: ${rsContext.stabilizationProbability}%

LEGAL STABILIZATION INDICATORS:
${rsContext.legalIndicators.map(indicator => 
  `- ${indicator.type}: ${indicator.description} (Confidence: ${indicator.confidence}%)`
).join('\n')}

NEIGHBORHOOD STABILIZATION ANALYSIS:
Total Stabilized Buildings in Area: ${rsContext.neighborhoodAnalysis.stabilizedCount}
Stabilization Rate: ${rsContext.neighborhoodAnalysis.stabilizationRate?.toFixed(1)}%
Average Building Age: ${rsContext.neighborhoodAnalysis.avgBuildingAge} years
Pre-1974 Buildings (Auto-Stabilized): ${rsContext.neighborhoodAnalysis.pre1974Count}

BED/BATH DISTRIBUTION IN MARKET:
${Object.entries(market.bedBathDistribution).map(([key, count]) => 
  `${key}: ${count} properties`
).join(', ')}

AMENITY FREQUENCY ANALYSIS:
${Object.entries(market.amenityFrequency).slice(0, 10).map(([amenity, count]) => 
  `${amenity}: ${count} properties (${((count/enhancedContext.totalComparables)*100).toFixed(1)}%)`
).join(', ')}

DATA QUALITY METRICS:
Total Samples: ${market.dataQuality.totalSamples}
With Square Footage: ${market.dataQuality.withSqft} (${((market.dataQuality.withSqft/market.dataQuality.totalSamples)*100).toFixed(1)}%)
With Amenities: ${market.dataQuality.withAmenities} (${((market.dataQuality.withAmenities/market.dataQuality.totalSamples)*100).toFixed(1)}%)
Data Completeness: ${market.dataQuality.completeness.toFixed(1)}%

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
     * Build comprehensive sales reasoning from Claude's detailed analysis
     */
    buildComprehensiveSalesReasoning(analysis, targetProperty, context) {
        const segments = [];
        
        // Market positioning with specific data
        if (analysis.keyMetrics?.pricePercentile) {
            segments.push(
                `Property is ${analysis.discountPercent}% below estimated market value of ${analysis.estimatedMarketPrice?.toLocaleString()}, ` +
                `ranking in the ${analysis.keyMetrics.pricePercentile}th percentile for ${targetProperty.neighborhood}.`
            );
        } else {
            segments.push(
                `Property is ${analysis.discountPercent}% below estimated market value of ${analysis.estimatedMarketPrice?.toLocaleString()} ` +
                `based on analysis of ${context.totalComparables} comparable properties.`
            );
        }
        
        // Detailed market comparison
        if (analysis.detailedAnalysis?.marketComparison) {
            segments.push(analysis.detailedAnalysis.marketComparison);
        }
        
        // Amenity and building analysis
        if (analysis.detailedAnalysis?.amenityAnalysis) {
            segments.push(analysis.detailedAnalysis.amenityAnalysis);
        }
        
        // Investment merit
        if (analysis.detailedAnalysis?.investmentMerit) {
            segments.push(analysis.detailedAnalysis.investmentMerit);
        }
        
        // Risk considerations
        if (analysis.detailedAnalysis?.riskFactors && analysis.confidence < 85) {
            segments.push(`Key risks: ${analysis.detailedAnalysis.riskFactors}`);
        }
        
        // Confidence and methodology
        const methodology = analysis.comparableInsights?.bestComparables?.length ? 
            `${analysis.comparableInsights.bestComparables.length} high-quality comparables` : 
            `${context.totalComparables} market comparables`;
            
        segments.push(
            `Analysis confidence: ${analysis.confidence}% using ${analysis.keyMetrics?.investmentGrade || 'B'}-grade ` +
            `methodology with ${methodology} and ${analysis.keyMetrics?.buildingQuality || 5}/10 building quality assessment.`
        );
        
        return segments.join(' ');
    }

    /**
     * Build comprehensive rentals reasoning from Claude's detailed analysis
     */
    buildComprehensiveRentalsReasoning(analysis, targetProperty, context) {
        const segments = [];
        
        // Market positioning with percentile
        if (analysis.keyMetrics?.rentPercentile) {
            segments.push(
                `Rental is ${analysis.percentBelowMarket}% below estimated market rent of ${analysis.estimatedMarketRent?.toLocaleString()}/month, ` +
                `ranking in the ${analysis.keyMetrics.rentPercentile}th percentile for ${targetProperty.neighborhood}.`
            );
        } else {
            segments.push(
                `Rental is ${analysis.percentBelowMarket}% below estimated market rent of ${analysis.estimatedMarketRent?.toLocaleString()}/month ` +
                `based on analysis of ${context.totalComparables} comparable rentals.`
            );
        }
        
        // Rent stabilization analysis
        segments.push(
            `Rent stabilization probability: ${analysis.rentStabilizedProbability}% based on ` +
            `${analysis.rentStabilizedFactors?.length || 0} legal indicators including ${analysis.rentStabilizedFactors?.slice(0, 2).join(' and ') || 'building characteristics'}.`
        );
        
        // Market comparison
        if (analysis.detailedAnalysis?.rentAnalysis) {
            segments.push(analysis.detailedAnalysis.rentAnalysis);
        }
        
        // Investment value
        if (analysis.investmentAnalysis?.annualSavings) {
            segments.push(
                `Investment merit: ${analysis.investmentAnalysis.annualSavings.toLocaleString()} annual savings potential ` +
                `with ${analysis.investmentAnalysis?.tenantProtection || 'moderate'} tenant protection value.`
            );
        }
        
        // Risk and protection analysis
        if (analysis.detailedAnalysis?.riskFactors) {
            segments.push(`Risk considerations: ${analysis.detailedAnalysis.riskFactors}`);
        }
        
        // Confidence breakdown
        const dataQuality = analysis.confidenceFactors?.data_quality || analysis.confidence;
        const legalFramework = analysis.confidenceFactors?.legal_framework || analysis.rentStabilizedProbability;
        
        segments.push(
            `Analysis confidence: ${analysis.confidence}% (data quality: ${dataQuality}%, ` +
            `legal framework: ${legalFramework}%) using ${context.totalComparables} market comparables ` +
            `and comprehensive rent stabilization legal analysis.`
        );
        
        return segments.join(' ');
    }

    // Enhanced validation methods
    validateEnhancedSalesAnalysis(analysis) {
        const required = [
            'estimatedMarketPrice', 'discountPercent', 'confidence', 'reasoning',
            'detailedAnalysis', 'keyMetrics', 'comparableInsights'
        ];
        
        return required.every(field => analysis.hasOwnProperty(field)) &&
               typeof analysis.estimatedMarketPrice === 'number' &&
               typeof analysis.discountPercent === 'number' &&
               typeof analysis.confidence === 'number' &&
               analysis.estimatedMarketPrice > 0 &&
               analysis.confidence >= 0 && analysis.confidence <= 100;
    }

    validateEnhancedRentalsAnalysis(analysis) {
        const required = [
            'estimatedMarketRent', 'percentBelowMarket', 'confidence', 
            'rentStabilizedProbability', 'reasoning', 'detailedAnalysis'
        ];
        
        return required.every(field => analysis.hasOwnProperty(field)) &&
               typeof analysis.estimatedMarketRent === 'number' &&
               typeof analysis.percentBelowMarket === 'number' &&
               typeof analysis.confidence === 'number' &&
               typeof analysis.rentStabilizedProbability === 'number' &&
               analysis.estimatedMarketRent > 0 &&
               analysis.confidence >= 0 && analysis.confidence <= 100 &&
               analysis.rentStabilizedProbability >= 0 && analysis.rentStabilizedProbability <= 100;
    }

    // Enhanced helper methods for comprehensive analysis
    calculateSimilarityScore(comparable, target) {
        let score = 0;
        
        // Bed/bath match (40% weight)
        if (comparable.bedrooms === target.bedrooms) score += 40;
        if (Math.abs(comparable.bathrooms - target.bathrooms) <= 0.5) score += 20;
        
        // Size similarity (20% weight)
        if (comparable.sqft && target.sqft) {
            const sizeDiff = Math.abs(comparable.sqft - target.sqft) / target.sqft;
            score += Math.max(0, 20 - (sizeDiff * 100));
        }
        
        // Amenity similarity (25% weight)
        const targetAmenities = new Set(target.amenities || []);
        const compAmenities = new Set(comparable.amenities || []);
        const intersection = new Set([...targetAmenities].filter(x => compAmenities.has(x)));
        const union = new Set([...targetAmenities, ...compAmenities]);
        if (union.size > 0) {
            score += (intersection.size / union.size) * 25;
        }
        
        // Building age similarity (15% weight)
        if (comparable.builtIn && target.builtIn) {
            const ageDiff = Math.abs(comparable.builtIn - target.builtIn);
            score += Math.max(0, 15 - (ageDiff / 10));
        }
        
        return Math.min(10, Math.max(0, score / 10));
    }

    analyzeComparablesByTiers(comparables, target, type) {
        const prices = comparables.map(c => type === 'sales' ? c.salePrice : c.price).filter(p => p > 0);
        const sortedPrices = [...prices].sort((a, b) => a - b);
        
        const tiers = [
            {
                description: 'Premium (Top 25%)',
                threshold: this.calculatePercentile(sortedPrices, 75),
                count: 0,
                avgPrice: 0,
                avgPsf: 0
            },
            {
                description: 'Mid-High (50-75%)',
                threshold: this.calculatePercentile(sortedPrices, 50),
                count: 0,
                avgPrice: 0,
                avgPsf: 0
            },
            {
                description: 'Mid-Market (25-50%)',
                threshold: this.calculatePercentile(sortedPrices, 25),
                count: 0,
                avgPrice: 0,
                avgPsf: 0
            },
            {
                description: 'Value (Bottom 25%)',
                threshold: 0,
                count: 0,
                avgPrice: 0,
                avgPsf: 0
            }
        ];

        // Categorize comparables into tiers
        comparables.forEach(comp => {
            const price = type === 'sales' ? comp.salePrice : comp.price;
            const psf = comp.sqft > 0 ? price / comp.sqft : 0;
            
            for (let i = 0; i < tiers.length; i++) {
                if (price >= tiers[i].threshold) {
                    tiers[i].count++;
                    tiers[i].avgPrice += price;
                    if (psf > 0) tiers[i].avgPsf += psf;
                    break;
                }
            }
        });

        // Calculate averages
        tiers.forEach(tier => {
            if (tier.count > 0) {
                tier.avgPrice = tier.avgPrice / tier.count;
                tier.avgPsf = tier.avgPsf / tier.count;
            }
        });

        return { tiers };
    }

    getNeighborhoodContext(neighborhood) {
        // Enhanced neighborhood mapping with investment insights
        const neighborhoodData = {
            'soho': { tier: 'Ultra-Premium', desirabilityScore: 10, pricePremium: '+40-50%', buyerProfile: 'Ultra-High Net Worth', velocity: 'Fast', investmentOutlook: 'Strong Appreciation' },
            'tribeca': { tier: 'Ultra-Premium', desirabilityScore: 10, pricePremium: '+35-45%', buyerProfile: 'Ultra-High Net Worth', velocity: 'Fast', investmentOutlook: 'Strong Appreciation' },
            'west-village': { tier: 'Ultra-Premium', desirabilityScore: 9, pricePremium: '+35-45%', buyerProfile: 'High Net Worth', velocity: 'Fast', investmentOutlook: 'Strong Appreciation' },
            'greenwich-village': { tier: 'Ultra-Premium', desirabilityScore: 9, pricePremium: '+35-45%', buyerProfile: 'High Net Worth', velocity: 'Fast', investmentOutlook: 'Strong Appreciation' },
            'dumbo': { tier: 'Premium', desirabilityScore: 8, pricePremium: '+25-35%', buyerProfile: 'High Net Worth', velocity: 'Fast', investmentOutlook: 'Strong Growth' },
            'brooklyn-heights': { tier: 'Premium', desirabilityScore: 8, pricePremium: '+25-35%', buyerProfile: 'Affluent Professional', velocity: 'Fast', investmentOutlook: 'Steady Growth' },
            'park-slope': { tier: 'High-End', desirabilityScore: 8, pricePremium: '+20-30%', buyerProfile: 'Affluent Family', velocity: 'Moderate-Fast', investmentOutlook: 'Steady Growth' },
            'williamsburg': { tier: 'High-End', desirabilityScore: 7, pricePremium: '+15-25%', buyerProfile: 'Young Professional', velocity: 'Fast', investmentOutlook: 'Growth Potential' },
            'long-island-city': { tier: 'Emerging', desirabilityScore: 6, pricePremium: '+10-20%', buyerProfile: 'Young Professional', velocity: 'Fast', investmentOutlook: 'High Growth' },
            'astoria': { tier: 'Emerging', desirabilityScore: 6, pricePremium: '+5-15%', buyerProfile: 'Middle Class', velocity: 'Moderate', investmentOutlook: 'Growth Potential' }
        };

        return neighborhoodData[neighborhood.toLowerCase()] || {
            tier: 'Mid-Market',
            desirabilityScore: 5,
            pricePremium: '+0-10%',
            buyerProfile: 'Mixed',
            velocity: 'Moderate',
            investmentOutlook: 'Stable'
        };
    }

    analyzeBedBathDistribution(comparables) {
        const distribution = {};
        comparables.forEach(comp => {
            const key = `${comp.bedrooms || 0}BR/${comp.bathrooms || 0}BA`;
            distribution[key] = (distribution[key] || 0) + 1;
        });
        return distribution;
    }

    analyzeAmenityFrequency(comparables) {
        const frequency = {};
        comparables.forEach(comp => {
            (comp.amenities || []).forEach(amenity => {
                frequency[amenity] = (frequency[amenity] || 0) + 1;
            });
        });
        return frequency;
    }

    analyzePriceDistribution(prices) {
        const sorted = [...prices].sort((a, b) => a - b);
        return {
            min: sorted[0],
            max: sorted[sorted.length - 1],
            q1: this.calculatePercentile(sorted, 25),
            median: this.calculatePercentile(sorted, 50),
            q3: this.calculatePercentile(sorted, 75),
            count: prices.length
        };
    }

    calculateDataCompleteness(comparables) {
        const totalFields = comparables.length * 6; // address, price, bed, bath, sqft, amenities
        let completedFields = 0;
        
        comparables.forEach(comp => {
            if (comp.address) completedFields++;
            if (comp.price > 0 || comp.salePrice > 0) completedFields++;
            if (comp.bedrooms !== undefined) completedFields++;
            if (comp.bathrooms !== undefined) completedFields++;
            if (comp.sqft > 0) completedFields++;
            if ((comp.amenities || []).length > 0) completedFields++;
        });
        
        return (completedFields / totalFields) * 100;
    }

    calculateSpaceEfficiency(property) {
        const sqft = property.sqft || 0;
        const bedrooms = property.bedrooms || 0;
        
        if (sqft === 0 || bedrooms === 0) return 50; // Neutral score
        
        const sqftPerBedroom = sqft / bedrooms;
        
        // Efficiency scoring based on sqft per bedroom
        if (sqftPerBedroom >= 500) return 85; // Very efficient
        if (sqftPerBedroom >= 400) return 75; // Good efficiency
        if (sqftPerBedroom >= 300) return 65; // Moderate efficiency
        if (sqftPerBedroom >= 200) return 45; // Below average
        return 25; // Poor efficiency
    }

    calculateAmenityScore(amenities) {
        const premiumAmenities = [
            'doorman', 'concierge', 'gym', 'pool', 'roof_deck', 'parking',
            'elevator', 'washer_dryer', 'dishwasher', 'central_air', 'balcony'
        ];
        
        let score = 0;
        amenities.forEach(amenity => {
            if (premiumAmenities.some(premium => amenity.toLowerCase().includes(premium))) {
                score += 10;
            } else {
                score += 2; // Other amenities get small bonus
            }
        });
        
        return Math.min(100, score);
    }

    analyzeDescription(description) {
        const text = description.toLowerCase();
        const highlights = [];
        const concerns = [];
        
        // Positive indicators
        const positiveKeywords = [
            'renovated', 'luxury', 'modern', 'updated', 'spacious', 'bright',
            'charming', 'elegant', 'pristine', 'stunning', 'gorgeous'
        ];
        
        // Negative indicators
        const negativeKeywords = [
            'needs work', 'tlc', 'fixer', 'as-is', 'original condition',
            'updating needed', 'potential'
        ];
        
        positiveKeywords.forEach(keyword => {
            if (text.includes(keyword)) highlights.push(keyword);
        });
        
        negativeKeywords.forEach(keyword => {
            if (text.includes(keyword)) concerns.push(keyword);
        });
        
        return { highlights, concerns };
    }

    assessBuildingQuality(property) {
        const builtIn = property.builtIn;
        const amenities = property.amenities || [];
        
        let score = 5; // Base score
        
        // Age factor
        if (builtIn) {
            if (builtIn >= 2010) score += 2; // New construction
            else if (builtIn >= 1980) score += 1; // Modern
            else if (builtIn >= 1945) score -= 1; // Post-war
            else score += 1; // Pre-war character
        }
        
        // Amenity factor
        const premiumAmenities = ['doorman', 'elevator', 'gym', 'pool'];
        const premiumCount = amenities.filter(a => 
            premiumAmenities.some(p => a.toLowerCase().includes(p))
        ).length;
        
        score += premiumCount;
        
        return Math.min(10, Math.max(1, score));
    }

    analyzeLocationFactors(property) {
        const address = (property.address || '').toLowerCase();
        const factors = [];
        
        // Street type indicators
        if (address.includes('avenue') || address.includes('ave')) {
            factors.push('major_avenue');
        }
        if (address.includes('street') || address.includes('st')) {
            factors.push('residential_street');
        }
        
        return factors;
    }

    assessMarketPosition(property, type) {
        const price = type === 'sales' ? property.salePrice : property.price;
        const amenityCount = (property.amenities || []).length;
        
        if (price >= 2000000 || (type === 'rentals' && price >= 6000)) {
            return amenityCount >= 5 ? 'ultra-luxury' : 'luxury';
        } else if (price >= 1000000 || (type === 'rentals' && price >= 4000)) {
            return amenityCount >= 3 ? 'luxury' : 'mid-luxury';
        } else if (price >= 500000 || (type === 'rentals' && price >= 2500)) {
            return 'mid-market';
        } else {
            return 'value';
        }
    }

    assessConditionFromDescription(description) {
        const text = description.toLowerCase();
        
        if (text.includes('gut renovated') || text.includes('mint condition')) return 95;
        if (text.includes('renovated') || text.includes('updated')) return 85;
        if (text.includes('move-in ready')) return 75;
        if (text.includes('good condition')) return 65;
        if (text.includes('needs updating') || text.includes('tlc')) return 35;
        if (text.includes('fixer') || text.includes('as-is')) return 15;
        
        return 60; // Neutral if no indicators
    }

    extractUniqueFeatures(description, amenities) {
        const features = [];
        const text = description.toLowerCase();
        
        // Unique architectural features
        const architecturalFeatures = [
            'exposed brick', 'high ceilings', 'loft', 'duplex', 'penthouse',
            'corner unit', 'south facing', 'river view', 'park view'
        ];
        
        architecturalFeatures.forEach(feature => {
            if (text.includes(feature)) features.push(feature);
        });
        
        return features;
    }

    buildEnhancedRentStabilizationContext(targetProperty, rentStabilizedBuildings, neighborhood) {
        const buildingMatches = this.findAllRentStabilizedMatches(targetProperty, rentStabilizedBuildings);
        const neighborhoodAnalysis = this.analyzeNeighborhoodStabilization(neighborhood, rentStabilizedBuildings);
        const legalAnalysis = this.performLegalStabilizationAnalysis(targetProperty);
        
        return {
            buildingMatches: buildingMatches,
            strongestMatch: buildingMatches[0] || null,
            matchConfidence: buildingMatches.length > 0 ? this.calculateMatchConfidence(buildingMatches[0], targetProperty) : 0,
            neighborhoodAnalysis: neighborhoodAnalysis,
            legalIndicators: legalAnalysis,
            registrationStatus: this.determineRegistrationStatus(buildingMatches),
            stabilizationProbability: this.calculateStabilizationProbability(targetProperty, buildingMatches, legalAnalysis)
        };
    }

    findAllRentStabilizedMatches(property, rentStabilizedBuildings) {
        const normalizedAddress = this.normalizeAddress(property.address);
        const matches = [];
        
        rentStabilizedBuildings.forEach(building => {
            const buildingAddress = this.normalizeAddress(building.address || '');
            if (buildingAddress && normalizedAddress.includes(buildingAddress)) {
                matches.push({
                    ...building,
                    confidence: this.calculateAddressMatchConfidence(normalizedAddress, buildingAddress)
                });
            }
        });
        
        return matches.sort((a, b) => b.confidence - a.confidence);
    }

    analyzeNeighborhoodStabilization(neighborhood, rentStabilizedBuildings) {
        const neighborhoodBuildings = rentStabilizedBuildings.filter(building => {
            const buildingAddress = this.normalizeAddress(building.address || '');
            const normalizedNeighborhood = this.normalizeAddress(neighborhood);
            return buildingAddress.includes(normalizedNeighborhood);
        });
        
        const totalBuildings = neighborhoodBuildings.length;
        const pre1974Buildings = neighborhoodBuildings.filter(b => 
            b.builtIn && b.builtIn < 1974
        ).length;
        
        const avgBuildingAge = neighborhoodBuildings.length > 0 ? 
            neighborhoodBuildings.reduce((sum, b) => sum + (b.builtIn || 1950), 0) / neighborhoodBuildings.length : 1970;
        
        return {
            stabilizedCount: totalBuildings,
            pre1974Count: pre1974Buildings,
            stabilizationRate: totalBuildings > 0 ? (totalBuildings / (totalBuildings + 100)) * 100 : 20, // Rough estimate
            avgBuildingAge: Math.round(new Date().getFullYear() - avgBuildingAge)
        };
    }

    performLegalStabilizationAnalysis(property) {
        const indicators = [];
        const description = (property.description || '').toLowerCase();
        
        // Explicit indicators
        const explicitTerms = ['rent stabilized', 'rent-stabilized', 'preferential rent', 'dhcr'];
        explicitTerms.forEach(term => {
            if (description.includes(term)) {
                indicators.push({
                    type: 'explicit',
                    description: `Explicit mention: "${term}"`,
                    confidence: 95
                });
            }
        });
        
        // Building characteristic indicators
        if (property.builtIn && property.builtIn < 1974) {
            indicators.push({
                type: 'building_age',
                description: 'Pre-1974 construction (automatic RSC coverage if 6+ units)',
                confidence: 85
            });
        }
        
        // Market rate indicators
        // This would be enhanced with actual market comparison
        
        return indicators;
    }

    calculateMatchConfidence(match, property) {
        if (!match) return 0;
        
        const addressSimilarity = this.calculateAddressMatchConfidence(
            this.normalizeAddress(property.address),
            this.normalizeAddress(match.address || '')
        );
        
        // Additional factors could include building characteristics
        return addressSimilarity;
    }

    calculateAddressMatchConfidence(target, building) {
        const targetWords = target.split(' ').filter(w => w.length > 2);
        const buildingWords = building.split(' ').filter(w => w.length > 2);
        
        const matches = targetWords.filter(word => buildingWords.includes(word));
        const confidence = (matches.length / Math.max(targetWords.length, buildingWords.length)) * 100;
        
        return Math.min(100, confidence);
    }

    determineRegistrationStatus(matches) {
        if (matches.length === 0) return 'not_found';
        if (matches[0].confidence >= 80) return 'confirmed';
        if (matches[0].confidence >= 60) return 'likely';
        return 'possible';
    }

    calculateStabilizationProbability(property, matches, legalAnalysis) {
        let probability = 0;
        
        // Building database match
        if (matches.length > 0) {
            probability += matches[0].confidence * 0.6; // 60% weight
        }
        
        // Legal indicators
        const explicitIndicators = legalAnalysis.filter(i => i.type === 'explicit');
        const buildingIndicators = legalAnalysis.filter(i => i.type === 'building_age');
        
        if (explicitIndicators.length > 0) {
            probability = Math.max(probability, 90);
        } else if (buildingIndicators.length > 0) {
            probability += 40; // Building age factor
        }
        
        // Market rate factor (simplified)
        // In practice, this would compare to actual market rates
        
        return Math.min(100, Math.max(0, probability));
    }

    // Enhanced statistical calculations
    calculateStandardDeviation(numbers) {
        if (numbers.length === 0) return 0;
        const mean = numbers.reduce((a, b) => a + b, 0) / numbers.length;
        const squaredDiffs = numbers.map(num => Math.pow(num - mean, 2));
        const variance = squaredDiffs.reduce((a, b) => a + b, 0) / numbers.length;
        return Math.sqrt(variance);
    }

    calculatePercentile(sortedArray, percentile) {
        if (sortedArray.length === 0) return 0;
        const index = (percentile / 100) * (sortedArray.length - 1);
        const lower = Math.floor(index);
        const upper = Math.ceil(index);
        const weight = index % 1;
        
        if (upper >= sortedArray.length) return sortedArray[sortedArray.length - 1];
        return sortedArray[lower] * (1 - weight) + sortedArray[upper] * weight;
    }

    extractDataFromResponse(responseText) {
        // Fallback data extraction if JSON parsing fails
        const patterns = {
            estimatedMarketPrice: /estimatedMarketPrice["\s:]+(\d+)/i,
            discountPercent: /discountPercent["\s:]+(\d+\.?\d*)/i,
            confidence: /confidence["\s:]+(\d+)/i,
            rentStabilizedProbability: /rentStabilizedProbability["\s:]+(\d+)/i
        };
        
        const extracted = {};
        
        for (const [key, pattern] of Object.entries(patterns)) {
            const match = responseText.match(pattern);
            if (match) {
                extracted[key] = parseFloat(match[1]);
            }
        }
        
        // Only return if we have essential fields
        if (extracted.estimatedMarketPrice || extracted.estimatedMarketRent) {
            return {
                ...extracted,
                reasoning: "Extracted from response - limited data available",
                detailedAnalysis: {},
                keyMetrics: {}
            };
        }
        
        return null;
    }

    // Utility methods from original
    calculateGradeFromDiscount(discountPercent) {
        if (discountPercent >= 25) return 'A+';
        if (discountPercent >= 20) return 'A';
        if (discountPercent >= 17) return 'A-';
        if (discountPercent >= 15) return 'B+';
        if (discountPercent >= 12) return 'B';
        if (discountPercent >= 10) return 'B-';
        if (discountPercent >= 7) return 'C+';
        if (discountPercent >= 5) return 'C';
        return 'C-';
    }

    normalizeAddress(address) {
        return address.toLowerCase()
            .replace(/[^\w\s]/g, ' ')
            .replace(/\s+/g, ' ')
            .trim();
    }

    calculateAverage(numbers) {
        if (numbers.length === 0) return 0;
        return numbers.reduce((sum, num) => sum + num, 0) / numbers.length;
    }

    calculateMedian(numbers) {
        if (numbers.length === 0) return 0;
        const sorted = [...numbers].sort((a, b) => a - b);
        const mid = Math.floor(sorted.length / 2);
        return sorted.length % 2 === 0 
            ? (sorted[mid - 1] + sorted[mid]) / 2 
            : sorted[mid];
    }

    async delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    getUsageStats() {
        return {
            apiCallsUsed: this.apiCallsUsed,
            cacheSize: this.neighborhoodCache.size
        };
    }
}

module.exports = EnhancedClaudeMarketAnalyzer;
