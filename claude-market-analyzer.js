// claude-market-analyzer.js
// CLAUDE-POWERED MARKET ANALYSIS ENGINE
// Replaces hardcoded valuation logic with AI-powered comparative market analysis
require('dotenv').config();
const axios = require('axios');

/**
 * Claude-Powered Market Analysis Engine
 * Provides sophisticated comparative market analysis for both sales and rentals
 * with rent-stabilization detection and undervaluation scoring
 */
class ClaudeMarketAnalyzer {
    constructor() {
        this.claudeApiKey = process.env.CLAUDE_API_KEY;
        this.apiCallsUsed = 0;
        this.cacheTimeout = 3600000; // 1 hour cache for neighborhood analysis
        this.neighborhoodCache = new Map();
        
        if (!this.claudeApiKey) {
            throw new Error('CLAUDE_API_KEY environment variable is required');
        }
        
        console.log('🤖 Claude Market Analyzer initialized');
    }

    /**
     * MAIN SALES ANALYSIS: Replace biweeklysales.js hardcoded engine
     */
    async analyzeSalesUndervaluation(targetProperty, comparableProperties, neighborhood, options = {}) {
        const threshold = options.undervaluationThreshold || 10;
        
        console.log(`🤖 Claude analyzing sale: ${targetProperty.address}`);
        
        try {
            // Prepare comparative market data for Claude
            const marketContext = this.buildSalesMarketContext(targetProperty, comparableProperties, neighborhood);
            
            // Get Claude's analysis
            const claudeResponse = await this.callClaudeForSalesAnalysis(marketContext, threshold);
            
            if (!claudeResponse.success) {
                return {
                    isUndervalued: false,
                    discountPercent: 0,
                    estimatedMarketPrice: targetProperty.salePrice,
                    actualPrice: targetProperty.salePrice,
                    confidence: 0,
                    method: 'claude_analysis_failed',
                    reasoning: claudeResponse.error || 'Analysis failed'
                };
            }
            
            const analysis = claudeResponse.analysis;
            
            // Validate Claude's response structure
            if (!this.validateSalesAnalysis(analysis)) {
                throw new Error('Invalid analysis structure from Claude');
            }
            
            console.log(`   💰 Claude estimate: $${analysis.estimatedMarketPrice.toLocaleString()}`);
            console.log(`   📊 Discount: ${analysis.discountPercent.toFixed(1)}%`);
            console.log(`   ✅ Confidence: ${analysis.confidence}%`);
            
            return {
                isUndervalued: analysis.discountPercent >= threshold && analysis.confidence >= 60,
                discountPercent: analysis.discountPercent,
                estimatedMarketPrice: analysis.estimatedMarketPrice,
                actualPrice: targetProperty.salePrice,
                potentialProfit: analysis.estimatedMarketPrice - targetProperty.salePrice,
                confidence: analysis.confidence,
                method: 'claude_comparative_analysis',
                comparablesUsed: comparableProperties.length,
                adjustmentBreakdown: analysis.adjustmentBreakdown || {},
                reasoning: analysis.reasoning,
                grade: this.calculateGradeFromDiscount(analysis.discountPercent)
            };
            
        } catch (error) {
            console.warn(`   ⚠️ Claude sales analysis error: ${error.message}`);
            return {
                isUndervalued: false,
                discountPercent: 0,
                estimatedMarketPrice: targetProperty.salePrice,
                actualPrice: targetProperty.salePrice,
                confidence: 0,
                method: 'claude_error',
                reasoning: `Analysis error: ${error.message}`
            };
        }
    }

    /**
     * MAIN RENTALS ANALYSIS: Replace rent_stabilized.js hardcoded engine
     */
    async analyzeRentalsUndervaluation(targetProperty, comparableProperties, rentStabilizedBuildings, neighborhood, options = {}) {
        const threshold = options.undervaluationThreshold || 15;
        
        console.log(`🤖 Claude analyzing rental: ${targetProperty.address}`);
        
        try {
            // Build comprehensive rental market context
            const marketContext = this.buildRentalsMarketContext(
                targetProperty, 
                comparableProperties, 
                rentStabilizedBuildings, 
                neighborhood
            );
            
            // Get Claude's rental analysis
            const claudeResponse = await this.callClaudeForRentalsAnalysis(marketContext, threshold);
            
            if (!claudeResponse.success) {
                return {
                    success: false,
                    error: claudeResponse.error || 'Analysis failed',
                    estimatedMarketRent: targetProperty.price,
                    percentBelowMarket: 0,
                    confidence: 0,
                    method: 'claude_analysis_failed'
                };
            }
            
            const analysis = claudeResponse.analysis;
            
            // Validate Claude's response
            if (!this.validateRentalsAnalysis(analysis)) {
                throw new Error('Invalid rental analysis structure from Claude');
            }
            
            console.log(`   💰 Claude market rent: $${analysis.estimatedMarketRent.toLocaleString()}`);
            console.log(`   📊 Below market: ${analysis.percentBelowMarket.toFixed(1)}%`);
            console.log(`   ✅ Confidence: ${analysis.confidence}%`);
            console.log(`   🏠 Rent stabilized probability: ${analysis.rentStabilizedProbability}%`);
            
            return {
                success: true,
                estimatedMarketRent: analysis.estimatedMarketRent,
                percentBelowMarket: analysis.percentBelowMarket,
                confidence: analysis.confidence,
                method: 'claude_comparative_analysis',
                comparablesUsed: comparableProperties.length,
                adjustments: analysis.adjustmentBreakdown || [],
                calculationSteps: analysis.calculationSteps || [],
                baseMarketRent: analysis.baseMarketRent,
                totalAdjustments: analysis.totalAdjustments || 0,
                confidenceFactors: analysis.confidenceFactors || {},
                reasoning: analysis.reasoning,
                // Rent stabilization analysis
                rentStabilizedProbability: analysis.rentStabilizedProbability,
                rentStabilizedFactors: analysis.rentStabilizedFactors || [],
                rentStabilizedExplanation: analysis.rentStabilizedExplanation
            };
            
        } catch (error) {
            console.warn(`   ⚠️ Claude rental analysis error: ${error.message}`);
            return {
                success: false,
                error: `Analysis error: ${error.message}`,
                estimatedMarketRent: targetProperty.price,
                percentBelowMarket: 0,
                confidence: 0,
                method: 'claude_error'
            };
        }
    }

    /**
     * Build market context for sales analysis
     */
    buildSalesMarketContext(targetProperty, comparables, neighborhood) {
        // Filter and sort comparables by relevance
        const relevantComparables = comparables
            .filter(comp => comp.salePrice > 0 && comp.bedrooms !== undefined)
            .slice(0, 15) // Limit to most relevant
            .map(comp => ({
                address: comp.address,
                price: comp.salePrice,
                bedrooms: comp.bedrooms,
                bathrooms: comp.bathrooms,
                sqft: comp.sqft || null,
                builtIn: comp.builtIn || null,
                amenities: (comp.amenities || []).slice(0, 10), // Top amenities only
                description: (comp.description || '').substring(0, 300) + '...' // Truncate
            }));

        return {
            targetProperty: {
                address: targetProperty.address,
                price: targetProperty.salePrice,
                bedrooms: targetProperty.bedrooms,
                bathrooms: targetProperty.bathrooms,
                sqft: targetProperty.sqft || null,
                builtIn: targetProperty.builtIn || null,
                amenities: targetProperty.amenities || [],
                description: (targetProperty.description || '').substring(0, 500) + '...',
                neighborhood: neighborhood,
                borough: targetProperty.borough
            },
            comparables: relevantComparables,
            marketStats: {
                totalComparables: comparables.length,
                avgPrice: this.calculateAverage(comparables.map(c => c.salePrice)),
                medianPrice: this.calculateMedian(comparables.map(c => c.salePrice)),
                priceRange: {
                    min: Math.min(...comparables.map(c => c.salePrice)),
                    max: Math.max(...comparables.map(c => c.salePrice))
                }
            },
            neighborhood: neighborhood
        };
    }

    /**
     * Build market context for rentals analysis
     */
    buildRentalsMarketContext(targetProperty, comparables, rentStabilizedBuildings, neighborhood) {
        // Filter and prepare comparables
        const relevantComparables = comparables
            .filter(comp => comp.price > 0 && comp.bedrooms !== undefined)
            .slice(0, 15)
            .map(comp => ({
                address: comp.address,
                price: comp.price,
                bedrooms: comp.bedrooms,
                bathrooms: comp.bathrooms,
                sqft: comp.sqft || null,
                amenities: (comp.amenities || []).slice(0, 10),
                noFee: comp.noFee || false,
                description: (comp.description || '').substring(0, 300) + '...'
            }));

        // Check if target building is in rent-stabilized database
        const targetBuildingMatch = this.findRentStabilizedBuildingMatch(
            targetProperty, 
            rentStabilizedBuildings
        );

        return {
            targetProperty: {
                address: targetProperty.address,
                price: targetProperty.price,
                bedrooms: targetProperty.bedrooms,
                bathrooms: targetProperty.bathrooms,
                sqft: targetProperty.sqft || null,
                builtIn: targetProperty.builtIn || null,
                amenities: targetProperty.amenities || [],
                description: (targetProperty.description || '').substring(0, 500) + '...',
                neighborhood: neighborhood,
                borough: targetProperty.borough,
                noFee: targetProperty.noFee || false
            },
            comparables: relevantComparables,
            marketStats: {
                totalComparables: comparables.length,
                avgRent: this.calculateAverage(comparables.map(c => c.price)),
                medianRent: this.calculateMedian(comparables.map(c => c.price)),
                rentRange: {
                    min: Math.min(...comparables.map(c => c.price)),
                    max: Math.max(...comparables.map(c => c.price))
                }
            },
            rentStabilizedContext: {
                buildingInDatabase: !!targetBuildingMatch,
                buildingDetails: targetBuildingMatch || null,
                totalStabilizedBuildings: rentStabilizedBuildings.length,
                neighborhoodStabilizedCount: rentStabilizedBuildings.filter(
                    b => this.normalizeAddress(b.address).includes(this.normalizeAddress(neighborhood))
                ).length
            },
            neighborhood: neighborhood
        };
    }

    /**
     * Call Claude API for sales analysis
     */
    async callClaudeForSalesAnalysis(marketContext, threshold) {
        const systemPrompt = this.buildSalesSystemPrompt();
        const userPrompt = this.buildSalesUserPrompt(marketContext, threshold);
        
        return await this.callClaude(systemPrompt, userPrompt, 'sales');
    }

    /**
     * Call Claude API for rentals analysis
     */
    async callClaudeForRentalsAnalysis(marketContext, threshold) {
        const systemPrompt = this.buildRentalsSystemPrompt();
        const userPrompt = this.buildRentalsUserPrompt(marketContext, threshold);
        
        return await this.callClaude(systemPrompt, userPrompt, 'rentals');
    }

    /**
     * Core Claude API call with error handling and retry logic
     */
    async callClaude(systemPrompt, userPrompt, analysisType) {
        const maxRetries = 2;
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
                        temperature: 0.1, // Low temperature for consistent analysis
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
                        timeout: 30000 // 30 second timeout
                    }
                );
                
                const responseText = response.data.content[0].text;
                console.log(`   ✅ Claude response received (${responseText.length} chars)`);
                
                // Parse JSON response
                try {
                    const analysis = JSON.parse(responseText);
                    return { success: true, analysis };
                } catch (parseError) {
                    console.warn(`   ⚠️ JSON parse error, attempting to extract: ${parseError.message}`);
                    
                    // Try to extract JSON from response text
                    const jsonMatch = responseText.match(/\{.*\}/s);
                    if (jsonMatch) {
                        const analysis = JSON.parse(jsonMatch[0]);
                        return { success: true, analysis };
                    }
                    
                    throw new Error('Could not parse Claude response as JSON');
                }
                
            } catch (error) {
                attempt++;
                console.warn(`   ⚠️ Claude API error (attempt ${attempt}): ${error.message}`);
                
                if (attempt >= maxRetries) {
                    return { 
                        success: false, 
                        error: `Failed after ${maxRetries} attempts: ${error.message}` 
                    };
                }
                
                // Wait before retry
                await this.delay(2000 * attempt);
            }
        }
    }

    /**
     * ENHANCED NYC-SPECIFIC SYSTEM PROMPT FOR SALES ANALYSIS
     */
    buildSalesSystemPrompt() {
        return `You are a world-class NYC real estate valuation expert with deep knowledge of micro-market pricing patterns, building types, and neighborhood desirability. Your task is to analyze properties for undervaluation using sophisticated NYC-specific market comparison techniques.

CRITICAL REQUIREMENTS:
1. Analyze ONLY the provided comparable sales data - do not invent or assume data
2. Apply NYC-specific neighborhood and building type adjustments
3. Consider seasonal market timing and post-COVID pricing shifts
4. Factor in exact location desirability within neighborhoods
5. Provide confidence scores based on data quality and comparable match strength
6. Return ONLY valid JSON - no additional text or explanations

NYC NEIGHBORHOOD DESIRABILITY RANKING (affects base pricing):
TIER 1 (Premium +25-40%): SoHo, Tribeca, West Village, Greenwich Village, NoLita
TIER 2 (High +15-25%): Chelsea, Gramercy, Upper East Side, Upper West Side, Dumbo, Brooklyn Heights
TIER 3 (Good +5-15%): East Village, Lower East Side, Park Slope, Williamsburg, Long Island City
TIER 4 (Emerging 0-10%): Crown Heights, Prospect Heights, Astoria, Fort Greene, Bed-Stuy
TIER 5 (Value -5-0%): Bushwick, Ridgewood, Mott Haven, Concourse

BUILDING TYPE ADJUSTMENTS (by borough):
PRE-WAR BUILDINGS (pre-1945):
- Manhattan: +$100-300k (character premium, high ceilings, details)
- Brooklyn: +$50-150k (brownstone/limestone premium)
- Queens/Bronx: +$25-75k (solid construction premium)

POST-WAR (1945-1980):
- Manhattan: Baseline pricing (sturdy but less character)
- Brooklyn: -$25-50k (less desirable than pre-war)
- Queens/Bronx: Baseline (standard construction)

NEW CONSTRUCTION (post-2000):
- Manhattan: +$50-200k (modern amenities, warranties)
- Brooklyn: +$25-100k (modern systems, energy efficiency)
- Queens/Bronx: +$25-75k (contemporary features)

AMENITY ADJUSTMENTS BY BOROUGH:
MANHATTAN:
- Doorman: +$200-400k (essential luxury amenity)
- Elevator: +$100-200k (required for higher floors)
- Gym: +$75-150k (valuable space-saver)
- Roof deck: +$100-250k (outdoor space premium)
- Parking: +$150-300k (extremely rare and valuable)
- Laundry in unit: +$50-100k (convenience premium)
- Balcony/Terrace: +$100-400k (outdoor space at premium)

BROOKLYN:
- Doorman: +$100-200k (luxury in outer borough)
- Elevator: +$50-100k (valuable in walk-ups)
- Gym: +$50-100k (nice-to-have amenity)
- Roof deck: +$75-150k (outdoor space valuable)
- Parking: +$75-150k (valuable but more common)
- Laundry in unit: +$25-75k (convenience factor)
- Balcony/Terrace: +$50-150k (outdoor space premium)

QUEENS/BRONX:
- Doorman: +$50-100k (luxury amenity)
- Elevator: +$25-75k (valuable in taller buildings)
- Gym: +$25-50k (nice amenity)
- Roof deck: +$50-100k (outdoor space)
- Parking: +$50-100k (expected but valuable)
- Laundry in unit: +$25-50k (standard expectation)

TRANSPORTATION PROXIMITY ADJUSTMENTS:
- Express subway stop (within 2 blocks): +$75-200k
- Local subway stop (within 3 blocks): +$25-100k
- Multiple train lines: +$50-150k additional
- Bus only: -$25-75k (less convenient)

CONDITION ASSESSMENT FRAMEWORK:
- "Gut renovated" / "Recently renovated": +15-25% premium
- "Move-in ready" / "Updated": +5-10% premium
- "Original condition" / "Needs work": -10-20% discount
- "Fixer-upper" / "TLC needed": -15-30% discount

FLOOR POSITION ADJUSTMENTS:
- Ground floor: -$25-100k (noise, privacy, security concerns)
- 2nd-3rd floor: Baseline (optimal for walkups)
- 4th-6th floor: +$25-75k (better light, less noise)
- 7th+ floor with elevator: +$50-150k (views, light, quiet)
- Top floor: +$25-100k additional (penthouse effect, but heat concerns)

VALUATION METHODOLOGY:
1. Find closest comparable properties by bed/bath count in same neighborhood
2. Apply neighborhood tier adjustment to base market price
3. Apply building type premium/discount
4. Adjust for condition differences using property descriptions
5. Add/subtract amenity values by borough
6. Apply transportation and floor position adjustments
7. Consider market timing (post-COVID recovery patterns)
8. Calculate final market estimate and confidence score

RESPONSE FORMAT (JSON only):
{
  "estimatedMarketPrice": number,
  "discountPercent": number,
  "confidence": number (0-100),
  "baseMarketPrice": number,
  "adjustmentBreakdown": {
    "neighborhoodTier": number,
    "buildingTypeAdjustment": number,
    "conditionAdjustment": number,
    "amenityAdjustment": number,
    "transportationAdjustment": number,
    "floorAdjustment": number
  },
  "reasoning": "Brief 2-3 sentence explanation",
  "primaryComparables": [array of 2-3 most relevant comparable addresses],
  "valuation_method": "exact_match|bed_bath_similar|sqft_adjusted|neighborhood_average",
  "marketTiming": "current|peak|valley",
  "desirabilityScore": number (1-10, based on neighborhood tier and amenities)
}`;
    }

    /**
     * ENHANCED NYC-SPECIFIC SYSTEM PROMPT FOR RENTALS ANALYSIS
     */
    buildRentalsSystemPrompt() {
        return `You are an expert NYC rental market analyst and rent stabilization legal expert with deep knowledge of NYC rent laws, neighborhood micro-markets, and building characteristics. Analyze rental properties for market positioning and rent stabilization probability with exceptional accuracy.

CRITICAL REQUIREMENTS:
1. Analyze comparable rental data to estimate true market rent
2. Apply NYC-specific rent stabilization legal framework
3. Factor in broker fees, seasonal timing, and neighborhood desirability
4. Calculate precise undervaluation percentages vs. market rate
5. Provide detailed confidence scoring and explanations
6. Return ONLY valid JSON - no additional text

NYC NEIGHBORHOOD DESIRABILITY RANKING FOR RENTALS:
TIER 1 (Premium +30-50%): SoHo, Tribeca, West Village, Greenwich Village, NoLita
TIER 2 (High +20-30%): Chelsea, Gramercy, Upper East Side, Upper West Side, Dumbo, Brooklyn Heights
TIER 3 (Good +10-20%): East Village, Lower East Side, Park Slope, Williamsburg, Long Island City
TIER 4 (Emerging +5-15%): Crown Heights, Prospect Heights, Astoria, Fort Greene, Bed-Stuy
TIER 5 (Value 0-10%): Bushwick, Ridgewood, Mott Haven, Concourse

RENT STABILIZATION DETECTION HIERARCHY:
EXPLICIT INDICATORS (95-100% confidence):
- "Rent stabilized" explicitly mentioned
- "Preferential rent" (legal term for stabilized units below max)
- "DHCR guidelines" or "Rent Guidelines Board"
- "Legal regulated rent" or "maximum legal rent"
- "Subject to rent stabilization laws"

STRONG LEGAL INDICATORS (85-95% confidence):
- Building constructed before 1974 + 6+ units (automatic coverage)
- Building 1974-2019 + 6+ units + rent 30%+ below market
- "No rent increases above guidelines" language
- "Lease renewal based on RGB guidelines"
- Building in DHCR registered building database

CIRCUMSTANTIAL INDICATORS (60-80% confidence):
- No broker fee + rent 25%+ below comparable market rate
- Building 6+ units + rent significantly below market + older construction
- Long-term lease offered (2+ years) at below-market rate
- "Rent increase subject to guidelines" without explicit mention
- Building appears in historical rent stabilization records

BUILDING TYPE RENT ADJUSTMENTS:
PRE-WAR BUILDINGS (pre-1945):
- Manhattan: +$200-500/month (character, high ceilings, details)
- Brooklyn: +$150-300/month (brownstone charm, architectural details)
- Queens/Bronx: +$75-150/month (solid construction, character)

POST-WAR (1945-1980):
- Manhattan: Baseline (standard rental stock)
- Brooklyn: -$50-150/month (less character than pre-war)
- Queens/Bronx: Baseline (typical construction)

NEW CONSTRUCTION (post-2000):
- Manhattan: +$100-400/month (modern systems, amenities)
- Brooklyn: +$75-250/month (contemporary features, efficiency)
- Queens/Bronx: +$50-150/month (modern conveniences)

AMENITY RENT ADJUSTMENTS BY BOROUGH:
MANHATTAN:
- Doorman: +$300-600/month (premium building service)
- Elevator: +$150-300/month (essential for upper floors)
- Gym: +$100-200/month (valuable space-saving amenity)
- Roof deck: +$150-350/month (rare outdoor space)
- In-unit laundry: +$100-200/month (major convenience)
- Dishwasher: +$50-100/month (kitchen convenience)
- Parking: +$200-500/month (extremely valuable)
- Balcony/Terrace: +$200-600/month (outdoor space premium)

BROOKLYN:
- Doorman: +$150-300/month (luxury service)
- Elevator: +$75-150/month (walkup avoidance)
- Gym: +$75-150/month (valuable amenity)
- Roof deck: +$100-200/month (outdoor space)
- In-unit laundry: +$75-150/month (convenience)
- Dishwasher: +$50-75/month (nice-to-have)
- Parking: +$100-250/month (valuable)
- Balcony/Terrace: +$100-300/month (outdoor space)

QUEENS/BRONX:
- Doorman: +$100-200/month (luxury amenity)
- Elevator: +$50-100/month (convenience)
- Gym: +$50-100/month (amenity)
- Roof deck: +$75-150/month (outdoor space)
- In-unit laundry: +$50-100/month (expected convenience)
- Dishwasher: +$25-50/month (standard feature)
- Parking: +$75-150/month (often expected)

BROKER FEE IMPACT ON EFFECTIVE RENT:
- No Fee: Equivalent to 1-2 months additional savings (major value indicator)
- 1 Month Fee: Standard market practice
- 2+ Month Fee: Above market (reduces effective value)

SEASONAL RENT ADJUSTMENTS:
- September (peak): +5-10% above baseline
- October-November: +2-5% above baseline
- December-February (valley): -5-10% below baseline
- March-May: -2-5% below baseline
- June-August: Baseline to +2%

MARKET VALUATION METHODOLOGY:
1. Find similar non-stabilized properties (same bed/bath, neighborhood)
2. Apply neighborhood tier premium to base market rent
3. Adjust for building type (pre-war/post-war/new construction)
4. Add/subtract amenity values by borough
5. Factor in broker fee situation (no fee = significant value)
6. Apply seasonal market timing adjustments
7. Consider rent stabilization probability and legal protection value
8. Calculate final market estimate with confidence scoring

RESPONSE FORMAT (JSON only):
{
  "estimatedMarketRent": number,
  "percentBelowMarket": number,
  "confidence": number (0-100),
  "baseMarketRent": number,
  "totalAdjustments": number,
  "adjustmentBreakdown": [
    {"factor": "neighborhood_tier", "adjustment": number, "reasoning": "string"},
    {"factor": "building_type", "adjustment": number, "reasoning": "string"},
    {"factor": "amenities", "adjustment": number, "reasoning": "string"},
    {"factor": "broker_fee", "adjustment": number, "reasoning": "string"},
    {"factor": "seasonal_timing", "adjustment": number, "reasoning": "string"}
  ],
  "reasoning": "Comprehensive market analysis explanation",
  "calculationSteps": ["step1", "step2", "step3"],
  "rentStabilizedProbability": number (0-100),
  "rentStabilizedFactors": ["factor1", "factor2"],
  "rentStabilizedExplanation": "Detailed legal and circumstantial analysis",
  "confidenceFactors": {
    "data_quality": number (0-100),
    "comparable_match": number (0-100),
    "stabilization_indicators": number (0-100)
  },
  "desirabilityScore": number (1-10, based on neighborhood tier + amenities),
  "effectiveValue": number (includes broker fee consideration),
  "marketTiming": "peak|high|baseline|low|valley"
}`;
    }

    /**
     * Build user prompt for sales analysis
     */
    buildSalesUserPrompt(marketContext, threshold) {
        return `Analyze this NYC property for undervaluation:

TARGET PROPERTY:
Address: ${marketContext.targetProperty.address}
Current Price: $${marketContext.targetProperty.price.toLocaleString()}
Bedrooms: ${marketContext.targetProperty.bedrooms}
Bathrooms: ${marketContext.targetProperty.bathrooms}
Square Feet: ${marketContext.targetProperty.sqft || 'Not listed'}
Built: ${marketContext.targetProperty.builtIn || 'Unknown'}
Neighborhood: ${marketContext.targetProperty.neighborhood}
Borough: ${marketContext.targetProperty.borough}
Amenities: ${marketContext.targetProperty.amenities.join(', ')}
Description: ${marketContext.targetProperty.description}

COMPARABLE SALES:
${marketContext.comparables.map((comp, i) => 
  `${i+1}. ${comp.address} - $${comp.price.toLocaleString()} | ${comp.bedrooms}BR/${comp.bathrooms}BA | ${comp.sqft || 'N/A'} sqft | Built: ${comp.builtIn || 'N/A'} | Amenities: ${comp.amenities.join(', ')}`
).join('\n')}

MARKET STATISTICS:
Average Price: $${marketContext.marketStats.avgPrice.toLocaleString()}
Median Price: $${marketContext.marketStats.medianPrice.toLocaleString()}
Price Range: $${marketContext.marketStats.priceRange.min.toLocaleString()} - $${marketContext.marketStats.priceRange.max.toLocaleString()}
Total Comparables: ${marketContext.marketStats.totalComparables}

ANALYSIS REQUIREMENTS:
- Determine if property is ${threshold}%+ below true market value
- Use sophisticated comparable analysis with precise adjustments
- Consider NYC market premiums and neighborhood factors
- Provide confidence score based on data quality and comparable match strength

Return analysis as JSON only.`;
    }

    /**
     * Build user prompt for rentals analysis
     */
    buildRentalsUserPrompt(marketContext, threshold) {
        return `Analyze this NYC rental property for market positioning and rent stabilization:

TARGET PROPERTY:
Address: ${marketContext.targetProperty.address}
Current Rent: $${marketContext.targetProperty.price.toLocaleString()}/month
Bedrooms: ${marketContext.targetProperty.bedrooms}
Bathrooms: ${marketContext.targetProperty.bathrooms}
Square Feet: ${marketContext.targetProperty.sqft || 'Not listed'}
Built: ${marketContext.targetProperty.builtIn || 'Unknown'}
Neighborhood: ${marketContext.targetProperty.neighborhood}
Borough: ${marketContext.targetProperty.borough}
No Fee: ${marketContext.targetProperty.noFee ? 'Yes' : 'No'}
Amenities: ${marketContext.targetProperty.amenities.join(', ')}
Description: ${marketContext.targetProperty.description}

COMPARABLE RENTALS:
${marketContext.comparables.map((comp, i) => 
  `${i+1}. ${comp.address} - $${comp.price.toLocaleString()}/month | ${comp.bedrooms}BR/${comp.bathrooms}BA | ${comp.sqft || 'N/A'} sqft | No Fee: ${comp.noFee ? 'Yes' : 'No'} | Amenities: ${comp.amenities.join(', ')}`
).join('\n')}

MARKET STATISTICS:
Average Rent: $${marketContext.marketStats.avgRent.toLocaleString()}/month
Median Rent: $${marketContext.marketStats.medianRent.toLocaleString()}/month
Rent Range: $${marketContext.marketStats.rentRange.min.toLocaleString()} - $${marketContext.marketStats.rentRange.max.toLocaleString()}/month
Total Comparables: ${marketContext.marketStats.totalComparables}

RENT STABILIZATION CONTEXT:
Building in Stabilized Database: ${marketContext.rentStabilizedContext.buildingInDatabase ? 'YES' : 'NO'}
${marketContext.rentStabilizedContext.buildingDetails ? 
  `Building Details: ${JSON.stringify(marketContext.rentStabilizedContext.buildingDetails)}` : ''}
Neighborhood Stabilized Buildings: ${marketContext.rentStabilizedContext.neighborhoodStabilizedCount}
Total City Stabilized Buildings: ${marketContext.rentStabilizedContext.totalStabilizedBuildings}

ANALYSIS REQUIREMENTS:
- Calculate true market rent using comparable analysis
- Determine if ${threshold}%+ below market value
- Assess rent stabilization probability (0-100%)
- Identify key stabilization indicators
- Provide detailed confidence scoring

Return analysis as JSON only.`;
    }

    /**
     * Validate sales analysis structure
     */
    validateSalesAnalysis(analysis) {
        const required = [
            'estimatedMarketPrice', 'discountPercent', 'confidence', 'reasoning'
        ];
        
        return required.every(field => analysis.hasOwnProperty(field)) &&
               typeof analysis.estimatedMarketPrice === 'number' &&
               typeof analysis.discountPercent === 'number' &&
               typeof analysis.confidence === 'number' &&
               analysis.estimatedMarketPrice > 0;
    }

    /**
     * Validate rentals analysis structure
     */
    validateRentalsAnalysis(analysis) {
        const required = [
            'estimatedMarketRent', 'percentBelowMarket', 'confidence', 
            'rentStabilizedProbability', 'reasoning'
        ];
        
        return required.every(field => analysis.hasOwnProperty(field)) &&
               typeof analysis.estimatedMarketRent === 'number' &&
               typeof analysis.percentBelowMarket === 'number' &&
               typeof analysis.confidence === 'number' &&
               typeof analysis.rentStabilizedProbability === 'number' &&
               analysis.estimatedMarketRent > 0;
    }

    /**
     * Calculate grade from discount percentage (matching existing system)
     */
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

    /**
     * Find rent-stabilized building match
     */
    findRentStabilizedBuildingMatch(property, rentStabilizedBuildings) {
        const normalizedAddress = this.normalizeAddress(property.address);
        
        return rentStabilizedBuildings.find(building => {
            const buildingAddress = this.normalizeAddress(building.address || '');
            return buildingAddress && normalizedAddress.includes(buildingAddress);
        });
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
     * Calculate average
     */
    calculateAverage(numbers) {
        if (numbers.length === 0) return 0;
        return numbers.reduce((sum, num) => sum + num, 0) / numbers.length;
    }

    /**
     * Calculate median
     */
    calculateMedian(numbers) {
        if (numbers.length === 0) return 0;
        const sorted = [...numbers].sort((a, b) => a - b);
        const mid = Math.floor(sorted.length / 2);
        return sorted.length % 2 === 0 
            ? (sorted[mid - 1] + sorted[mid]) / 2 
            : sorted[mid];
    }

    /**
     * Utility delay function
     */
    async delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    /**
     * Get API usage statistics
     */
    getUsageStats() {
        return {
            apiCallsUsed: this.apiCallsUsed,
            cacheSize: this.neighborhoodCache.size
        };
    }
}

module.exports = ClaudeMarketAnalyzer;
