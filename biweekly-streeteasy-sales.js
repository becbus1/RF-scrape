// FIXED VERSION: Claude AI Analysis + Smart Deduplication + Fixed Rate Limiting + Sold Detection Bug Fix
// FIXES: Rate limiting matches rentals, fixed sold detection logic, Claude JSON parsing, null reference errors
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

/**
 * CLAUDE AI SALES MARKET ANALYZER - FIXED JSON PARSING
 */
class ClaudeSalesMarketAnalyzer {
    constructor() {
        this.claudeApiKey = process.env.ANTHROPIC_API_KEY || process.env.CLAUDE_API_KEY;
        this.apiCallsUsed = 0;
        
        if (!this.claudeApiKey) {
            throw new Error('ANTHROPIC_API_KEY or CLAUDE_API_KEY environment variable is required');
        }
        
        console.log('🤖 Claude Sales Market Analyzer initialized');
    }

    /**
     * MAIN SALES ANALYSIS FUNCTION - FIXED NULL REFERENCE ERRORS
     */
    async analyzeSalesUndervaluation(targetProperty, comparableProperties, neighborhood, options = {}) {
        const threshold = options.undervaluationThreshold || 10;
        
        console.log(`🤖 Claude analyzing sale: ${targetProperty.address}`);
        
        try {
            // STEP 1: Pre-filter comparables using hierarchy
            const filteredComparables = this.filterSalesComparablesUsingHierarchy(targetProperty, comparableProperties);
            console.log(`   🎯 Filtered to ${filteredComparables.selectedComparables.length} specific matches using ${filteredComparables.method}`);
            
            // STEP 2: Build context with filtered comparables for Claude
            const enhancedContext = this.buildEnhancedSalesContext(targetProperty, filteredComparables.selectedComparables, neighborhood, options);
            
            // STEP 3: Let Claude analyze the specific comparables naturally
            const claudeResponse = await this.callClaudeForEnhancedSalesAnalysis(enhancedContext, threshold);
            
            // STEP 4: Parse Claude's response and add metadata - FIXED ERROR HANDLING
            const analysis = this.extractDataFromClaudeResponse(claudeResponse);
            
            // FIXED: Handle null analysis gracefully
            if (!analysis) {
                console.warn(`   ⚠️ Claude analysis returned invalid data for ${targetProperty.address}`);
                return this.createFallbackAnalysis(targetProperty, filteredComparables, threshold);
            }
            
            // STEP 5: Calculate final confidence based on method quality
            const methodConfidence = this.calculateConfidenceFromMethod(filteredComparables.method, filteredComparables.selectedComparables.length);
            const finalConfidence = Math.min(95, Math.max(analysis.confidence || methodConfidence, methodConfidence));
            
            // STEP 6: Map to standardized response format
            return this.mapSalesResponseToDatabase(analysis, targetProperty, filteredComparables, threshold, finalConfidence);
            
        } catch (error) {
            console.warn(`   ⚠️ Claude sales analysis error: ${error.message}`);
            return this.createFallbackAnalysis(targetProperty, { selectedComparables: comparableProperties, method: 'fallback' }, threshold);
        }
    }

    /**
     * FIXED: Create fallback analysis when Claude fails
     */
    createFallbackAnalysis(targetProperty, filteredComparables, threshold) {
        return {
            isUndervalued: false,
            discountPercent: 0,
            estimatedMarketPrice: targetProperty.salePrice || targetProperty.price || 0,
            actualPrice: targetProperty.salePrice || targetProperty.price || 0,
            potentialProfit: 0,
            confidence: 30,
            method: 'claude_analysis_failed',
            reasoning: 'Analysis failed - using conservative estimate',
            comparablesUsed: filteredComparables.selectedComparables?.length || 0,
            detailedAnalysis: {},
            adjustmentBreakdown: {},
            score: 0,
            grade: 'C-'
        };
    }

    /**
     * HIERARCHICAL COMPARABLE FILTERING FOR SALES
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
     * BUILD ENHANCED CONTEXT FOR CLAUDE SALES ANALYSIS
     */
    buildEnhancedSalesContext(targetProperty, comparables, neighborhood, options) {
        return {
            targetProperty: targetProperty,
            comparables: comparables,
            neighborhood: neighborhood,
            valuationMethod: this.getMethodDescription(comparables.length),
            analysisDate: new Date().toISOString(),
            options: options
        };
    }

    /**
     * CALL CLAUDE API FOR ENHANCED SALES ANALYSIS
     */
    async callClaudeForEnhancedSalesAnalysis(enhancedContext, threshold) {
        const systemPrompt = this.buildEnhancedSalesSystemPrompt();
        const userPrompt = this.buildEnhancedSalesUserPrompt(enhancedContext, threshold);
        
        return await this.callClaude(systemPrompt, userPrompt, 'sales');
    }

    /**
     * ENHANCED SYSTEM PROMPT FOR SALES ANALYSIS - FIXED JSON FORMAT
     */
    buildEnhancedSalesSystemPrompt() {
        return `You are an expert NYC real estate investment analyst. You provide natural analysis and MUST return valid JSON only.

CRITICAL: Your response must be ONLY valid JSON - no text before or after the JSON object.

ANALYSIS APPROACH:
- Analyze the property using the provided filtered comparable sales
- Calculate market value and potential savings
- Explain investment value in natural language
- Return ONLY the JSON response below

RESPONSE FORMAT - ONLY THIS JSON:
{
  "estimatedMarketPrice": number,
  "discountPercent": number,
  "baseMarketPrice": number,
  "potentialSavings": number,
  "confidence": number (30-95),
  "reasoning": "Natural explanation of value and market positioning",
  "detailedAnalysis": {
    "valueExplanation": "Why this property offers good/poor value",
    "comparableAnalysis": "How it compares to filtered properties",
    "amenityComparison": "Amenity differences vs comparables",
    "investmentFactors": "Investment factors affecting value",
    "marketTiming": "Market timing considerations"
  },
  "adjustmentBreakdown": {
    "amenities": number,
    "condition": number,
    "size": number,
    "location": number,
    "buildingType": number
  }
}

CRITICAL: Return ONLY valid JSON. No markdown, no extra text, just the JSON object.`;
    }

    /**
     * BUILD ENHANCED USER PROMPT FOR SALES ANALYSIS
     */
    buildEnhancedSalesUserPrompt(enhancedContext, threshold) {
        const target = enhancedContext.targetProperty;
        const comparables = enhancedContext.comparables;
        
        return `Analyze this NYC property for sale using the filtered comparable sales:

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
Amenities: ${target.amenities?.join(', ') || 'None listed'}

FILTERED COMPARABLE SALES (${enhancedContext.valuationMethod}):
${comparables.slice(0, 12).map((comp, i) => 
  `${i+1}. ${comp.address} - $${comp.salePrice?.toLocaleString() || comp.price?.toLocaleString()} | ${comp.bedrooms}BR/${comp.bathrooms}BA | ${comp.sqft || 'N/A'} sqft | Built: ${comp.builtIn || 'N/A'} | Amenities: ${comp.amenities?.slice(0, 4).join(', ') || 'None'}`
).join('\n')}

Compare this property to the ${comparables.length} filtered comparables. Calculate if sale price is ${threshold}%+ below market. Return ONLY valid JSON.`;
    }

    /**
     * CALL CLAUDE API WITH RETRY LOGIC
     */
    async callClaude(systemPrompt, userPrompt, analysisType) {
        const maxRetries = 3;
        let lastError;

        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                const response = await axios.post(
                    'https://api.anthropic.com/v1/messages',
                    {
                        model: 'claude-3-5-sonnet-20241022',
                        max_tokens: 2000,
                        temperature: 0.1,
                        system: systemPrompt,
                        messages: [
                            {
                                role: 'user',
                                content: userPrompt
                            }
                        ]
                    },
                    {
                        headers: {
                            'Content-Type': 'application/json',
                            'x-api-key': this.claudeApiKey,
                            'anthropic-version': '2023-06-01'
                        },
                        timeout: 30000
                    }
                );

                this.apiCallsUsed++;
                
                if (response.data?.content?.[0]?.text) {
                    return response.data.content[0].text;
                } else {
                    throw new Error('Invalid response format from Claude API');
                }

            } catch (error) {
                lastError = error;
                console.warn(`   ⚠️ Claude API attempt ${attempt}/${maxRetries} failed:`, error.message);
                
                if (attempt < maxRetries) {
                    const delay = Math.pow(2, attempt) * 1000; // Exponential backoff
                    await new Promise(resolve => setTimeout(resolve, delay));
                }
            }
        }

        throw new Error(`Claude API failed after ${maxRetries} attempts: ${lastError.message}`);
    }

    /**
     * EXTRACT DATA FROM CLAUDE RESPONSE - FIXED JSON PARSING
     */
    extractDataFromClaudeResponse(claudeResponse) {
        try {
            // Clean the response - remove any markdown or extra text
            let cleanedResponse = claudeResponse.trim();
            
            // Remove markdown code blocks if present
            cleanedResponse = cleanedResponse.replace(/```json\s*/g, '').replace(/```\s*/g, '');
            
            // Find JSON object in the response
            const jsonMatch = cleanedResponse.match(/\{[\s\S]*\}/);
            if (!jsonMatch) {
                console.warn('   ⚠️ No JSON found in Claude response');
                return null;
            }

            let jsonStr = jsonMatch[0];
            
            // FIXED: Clean up common JSON issues
            // Remove trailing commas
            jsonStr = jsonStr.replace(/,(\s*[}\]])/g, '$1');
            
            // Fix unquoted property names (common Claude error)
            jsonStr = jsonStr.replace(/(\w+):/g, '"$1":');
            
            // Fix single quotes
            jsonStr = jsonStr.replace(/'/g, '"');
            
            // Parse the cleaned JSON
            const extracted = JSON.parse(jsonStr);
            
            // Validate required fields
            if (typeof extracted.estimatedMarketPrice !== 'number' || 
                typeof extracted.discountPercent !== 'number' ||
                !extracted.reasoning) {
                console.warn('   ⚠️ Missing required fields in Claude response');
                return null;
            }

            // Ensure all numbers are valid
            extracted.estimatedMarketPrice = Number(extracted.estimatedMarketPrice) || 0;
            extracted.discountPercent = Number(extracted.discountPercent) || 0;
            extracted.confidence = Number(extracted.confidence) || 50;
            extracted.potentialSavings = Number(extracted.potentialSavings) || 0;

            return extracted;
            
        } catch (error) {
            console.warn(`   ⚠️ Data extraction failed: ${error.message}`);
            console.warn(`   📝 Claude response: ${claudeResponse.substring(0, 200)}...`);
            return null;
        }
    }

    /**
     * MAP SALES RESPONSE TO DATABASE STRUCTURE - FIXED NULL HANDLING
     */
    mapSalesResponseToDatabase(analysis, targetProperty, filteredComparables, threshold, finalConfidence) {
        // FIXED: Safe property access
        const actualPrice = targetProperty.salePrice || targetProperty.price || 0;
        const estimatedPrice = analysis?.estimatedMarketPrice || actualPrice;
        const discountPercent = analysis?.discountPercent || 0;
        const potentialProfit = analysis?.potentialSavings || 0;
        
        const isUndervalued = discountPercent >= threshold && finalConfidence >= 50;
        
        return {
            isUndervalued: isUndervalued,
            discountPercent: discountPercent,
            estimatedMarketPrice: estimatedPrice,
            actualPrice: actualPrice,
            potentialProfit: potentialProfit,
            confidence: finalConfidence,
            method: 'claude_hierarchical_analysis',
            comparablesUsed: filteredComparables.selectedComparables?.length || 0,
            reasoning: analysis?.reasoning || 'Claude AI market analysis',
            
            // Enhanced metrics for database integration
            detailedAnalysis: analysis?.detailedAnalysis || {},
            adjustmentBreakdown: analysis?.adjustmentBreakdown || {},
            valuationMethod: filteredComparables.method,
            
            // Database-specific fields
            score: this.calculateScoreFromSalesAnalysis(analysis, filteredComparables, finalConfidence),
            grade: this.calculateGradeFromScore(this.calculateScoreFromSalesAnalysis(analysis, filteredComparables, finalConfidence))
        };
    }

    /**
     * CALCULATE CONFIDENCE FROM METHOD QUALITY
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
     * CALCULATE SCORE FROM SALES ANALYSIS (0-100) - FIXED NULL HANDLING
     */
    calculateScoreFromSalesAnalysis(analysis, filteredComparables, confidence) {
        let score = 50; // Base score
        
        // Undervaluation bonus (0-40 points)
        const discountPercent = analysis?.discountPercent || 0;
        if (discountPercent >= 25) score += 40;
        else if (discountPercent >= 20) score += 30;
        else if (discountPercent >= 15) score += 20;
        else if (discountPercent >= 10) score += 10;
        
        // Method quality bonus (0-20 points)
        switch (filteredComparables.method) {
            case 'exact_bed_bath_amenity_match': score += 20; break;
            case 'bed_bath_specific_pricing': score += 15; break;
            case 'bed_specific_with_adjustments': score += 10; break;
            case 'price_per_sqft_fallback': score += 5; break;
        }
        
        // Sample size bonus (0-10 points)
        const sampleSize = filteredComparables.selectedComparables?.length || 0;
        if (sampleSize >= 20) score += 10;
        else if (sampleSize >= 15) score += 7;
        else if (sampleSize >= 10) score += 5;
        else if (sampleSize >= 5) score += 3;
        
        // Confidence bonus (0-10 points)
        if (confidence >= 90) score += 10;
        else if (confidence >= 80) score += 7;
        else if (confidence >= 70) score += 5;
        
        return Math.min(100, Math.max(0, Math.round(score)));
    }

    /**
     * CALCULATE GRADE FROM SCORE
     */
    calculateGradeFromScore(score) {
        if (score >= 90) return 'A+';
        if (score >= 85) return 'A';
        if (score >= 80) return 'A-';
        if (score >= 75) return 'B+';
        if (score >= 70) return 'B';
        if (score >= 65) return 'B-';
        if (score >= 60) return 'C+';
        if (score >= 55) return 'C';
        return 'C-';
    }

    /**
     * UTILITY FUNCTIONS
     */
    
    normalizeAmenities(amenities) {
        const amenityText = amenities.join(' ').toLowerCase();
        const normalized = [];
        
        const amenityMappings = {
            'doorman': ['doorman', 'full time doorman', 'full-time doorman'],
            'elevator': ['elevator', 'lift'],
            'washer_dryer': ['washer/dryer', 'washer dryer', 'w/d', 'laundry in unit'],
            'dishwasher': ['dishwasher'],
            'central_air': ['central air', 'central a/c', 'central ac'],
            'balcony': ['balcony', 'private balcony'],
            'terrace': ['terrace', 'private terrace'],
            'gym': ['gym', 'fitness center', 'fitness room'],
            'pool': ['pool', 'swimming pool'],
            'roof_deck': ['roof deck', 'rooftop'],
            'parking': ['parking', 'garage parking'],
            'pet_friendly': ['pets allowed', 'pet friendly', 'pet ok']
        };
        
        for (const [standardName, variations] of Object.entries(amenityMappings)) {
            if (variations.some(variation => amenityText.includes(variation))) {
                normalized.push(standardName);
            }
        }
        
        return [...new Set(normalized)];
    }

    hasSignificantAmenityOverlap(comp1Amenities, comp2Amenities) {
        const normalized1 = this.normalizeAmenities(comp1Amenities);
        const normalized2 = this.normalizeAmenities(comp2Amenities);
        
        if (normalized1.length === 0 && normalized2.length === 0) return true;
        if (normalized1.length === 0 || normalized2.length === 0) return false;
        
        const overlap = normalized1.filter(amenity => normalized2.includes(amenity));
        const overlapPercentage = overlap.length / Math.max(normalized1.length, normalized2.length);
        
        return overlapPercentage >= 0.5; // 50% overlap required
    }

    getMethodDescription(comparableCount) {
        return `${comparableCount} filtered comparable sales`;
    }
}

/**
 * ENHANCED BIWEEKLY SALES ANALYZER - FIXED RATE LIMITING AND SOLD DETECTION
 */
class EnhancedBiWeeklySalesAnalyzer {
    constructor() {
        this.supabase = createClient(
            process.env.SUPABASE_URL,
            process.env.SUPABASE_ANON_KEY
        );
        
        this.rapidApiKey = process.env.RAPIDAPI_KEY;
        this.apiCallsUsed = 0;
        
        // Initialize Claude AI analyzer
        this.claudeAnalyzer = new ClaudeSalesMarketAnalyzer();
        
        // Check for initial bulk load mode
        this.initialBulkLoad = process.env.INITIAL_BULK_LOAD === 'true';
        
        // Store deploy/startup time for delay calculation
        this.deployTime = new Date().getTime();
        
        // FIXED: RATE LIMITING MATCHES RENTALS EXACTLY
        this.baseDelay = this.initialBulkLoad ? 8000 : 6000;
        this.maxRetries = 3;
        this.retryBackoffMultiplier = 2;
        
        // Adaptive rate limiting tracking
        this.rateLimitHits = 0;
        this.callTimestamps = [];
        this.maxCallsPerHour = this.initialBulkLoad ? 250 : 300;
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
            // Deduplication performance tracking
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
     * Get today's neighborhood assignments WITH BULK LOAD SUPPORT + TEST_NEIGHBORHOOD
     */
    async getTodaysNeighborhoods() {
        // PRIORITY 1: Test neighborhood override
        if (process.env.TEST_NEIGHBORHOOD) {
            console.log(`🧪 TEST MODE: Using single neighborhood: ${process.env.TEST_NEIGHBORHOOD}`);
            console.log('⚡ This will run full Claude AI analysis on one neighborhood for testing');
            return [process.env.TEST_NEIGHBORHOOD];
        }
        
        // PRIORITY 2: Initial bulk load mode
        if (this.initialBulkLoad) {
            console.log('🚀 INITIAL BULK LOAD MODE: Processing ALL sales neighborhoods');
            console.log(`📋 Will process ${HIGH_PRIORITY_NEIGHBORHOODS.length} neighborhoods over multiple hours`);
            return HIGH_PRIORITY_NEIGHBORHOODS;
        }
        
        // PRIORITY 3: Normal bi-weekly schedule (for production)
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
     * FIXED: Update only price in cache (no refetch needed)
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
     * FIXED: Update price in undervalued_sales table if listing exists
     */
    async updatePriceInUndervaluedSales(listingId, newPrice, sqft) {
        try {
            const updateData = {
                price: parseInt(newPrice),
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
     * FIXED: Mark listing for reanalysis due to price change
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
     * FIXED: Handle price updates efficiently without refetching
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
                        
                        // Update price in cache without refetching
                        await this.updatePriceInCache(cachedEntry.listing_id, currentPrice);
                        
                        // Update price in undervalued_sales if exists
                        await this.updatePriceInUndervaluedSales(cachedEntry.listing_id, currentPrice, cachedEntry.sqft);
                        
                        // Trigger reanalysis for undervaluation (price changed)
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
     * FIXED: Run sold detection for specific neighborhood - FIXED LOGIC ERROR
     */
    async runSoldDetectionForNeighborhood(searchResults, neighborhood) {
        const currentTime = new Date().toISOString();
        const currentListingIds = searchResults.map(r => r.id?.toString()).filter(Boolean);
        
        try {
            // FIXED: Only check for listings in THIS SPECIFIC NEIGHBORHOOD
            const threeDaysAgo = new Date();
            threeDaysAgo.setDate(threeDaysAgo.getDate() - 3);

            // CRITICAL FIX: Only get sales from THIS neighborhood that weren't in current search
            const { data: missingSales, error: missingError } = await this.supabase
                .from('sales_market_cache')
                .select('listing_id')
                .eq('neighborhood', neighborhood) // FIXED: Only this neighborhood
                .not('listing_id', 'in', `(${currentListingIds.map(id => `"${id}"`).join(',')})`)
                .gte('last_seen_in_search', threeDaysAgo.toISOString()) // FIXED: Recent listings only
                .eq('market_status', 'active'); // FIXED: Only check active listings

            if (missingError) {
                console.warn('⚠️ Error checking for missing sales:', missingError.message);
                return { markedSold: 0 };
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
                    .eq('status', 'active')
                    .eq('neighborhood', neighborhood); // FIXED: Only this neighborhood

                if (!markSoldError) {
                    markedSold = missingIds.length;
                    this.apiUsageStats.listingsMarkedSold += markedSold;
                    if (markedSold > 0) {
                        console.log(`   🏠 Marked ${markedSold} sales as likely sold in ${neighborhood}`);
                    }
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
     * FIXED: Update only search timestamps for sold detection
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

            // Step 2: Run sold detection for this neighborhood ONLY
            const { markedSold } = await this.runSoldDetectionForNeighborhood(searchResults, neighborhood);
            
            console.log(`   💾 Updated search timestamps: ${searchResults.length} sales, marked ${markedSold} as sold`);
            return { updated: searchResults.length, markedSold };

        } catch (error) {
            console.warn('⚠️ Error updating search timestamps:', error.message);
            return { updated: 0, markedSold: 0 };
        }
    }

    /**
     * Cache complete sale details for new listings
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
     * Cache failed fetch attempt
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
     * Update cache with analysis results (mark as undervalued or market_rate)
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
        
        console.log(`\n📊 ${mode} CLAUDE AI SALES ANALYSIS COMPLETE`);
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
        console.log(`🤖 Claude AI calls: ${this.claudeAnalyzer.apiCallsUsed}`);
        
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
            console.log('\n🎯 CLAUDE AI SALES BULK LOAD COMPLETE!');
            console.log('📝 Next steps:');
            console.log('   1. Set INITIAL_BULK_LOAD=false in Railway');
            console.log('   2. Switch to bi-weekly maintenance mode');
            console.log('   3. Enjoy 75-90% API savings from smart caching!');
            console.log('   4. Claude AI provides natural language explanations for all deals');
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
            console.log('\n🎉 SUCCESS: Found undervalued sales with Claude AI analysis!');
            console.log(`🔍 Check your Supabase 'undervalued_sales' table for ${summary.savedToDatabase} new deals`);
            console.log(`🤖 All properties include Claude AI natural language explanations`);
            
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
     * FIXED: ADAPTIVE rate limiting - MATCHES RENTALS EXACTLY
     */
    adaptiveRateLimit() {
        const now = Date.now();
        
        // Clean old timestamps (older than 1 hour)
        this.callTimestamps = this.callTimestamps.filter(t => now - t < 60 * 60 * 1000);
        
        // Check if we're hitting hourly limits
        const callsThisHour = this.callTimestamps.length;
        
        // FIXED: ADAPTIVE LOGIC MATCHES RENTALS EXACTLY
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
     * FIXED: Enhanced delay with adaptive rate limiting - MATCHES RENTALS
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
     * Main bi-weekly sales refresh with CLAUDE AI + SMART DEDUPLICATION - FIXED
     */
    async runBiWeeklySalesRefresh() {
        console.log('\n🏠 CLAUDE AI SALES ANALYSIS WITH SMART DEDUPLICATION');
        console.log('🤖 FIXED: Claude AI natural language market analysis');
        console.log('🎯 FIXED: Hierarchical comparable filtering + JSON parsing');
        console.log('💾 FIXED: Cache-optimized sold detection (only current neighborhood)');
        console.log('🏠 FIXED: Rate limiting matches rentals exactly');
        console.log('⚡ FIXED: Adaptive rate limiting with proper delay logic');
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
            // Deduplication stats
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
                    
                    // Step 3: CLAUDE AI ANALYSIS for undervaluation
                    const undervaluedSales = await this.analyzeForAdvancedSalesUndervaluation(detailedSales, neighborhood);
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
                    console.log(`   ✅ ${neighborhood}: ${undervaluedSales.length} undervalued sales found (Claude AI analysis)`);

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
            console.error('💥 Claude AI sales refresh failed:', error.message);
            summary.errors.push({ error: error.message });
        }

        return { summary };
    }

    /**
     * FIXED: SMART DEDUPLICATION - Fetch active sales and identify which need detail fetching
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
                
                // HOA and taxes
                monthlyHoa: data.monthlyHoa || 0,
                monthlyTax: data.monthlyTax || 0,
                
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

    /**
     * CLAUDE AI SALES ANALYSIS - Main analysis function
     */
    async analyzeForAdvancedSalesUndervaluation(detailedSales, neighborhood) {
        if (detailedSales.length < 5) {
            console.log(`   ⚠️ Not enough sales (${detailedSales.length}) for analysis in ${neighborhood}`);
            return [];
        }

        console.log(`   🤖 CLAUDE AI ANALYSIS: ${detailedSales.length} sales using natural language reasoning...`);

        const undervaluedSales = [];

        // Analyze each sale using Claude AI
        for (const sale of detailedSales) {
            try {
                // Use Claude AI for natural language market analysis
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
                        
                        // Enhanced Claude fields
                        detailedAnalysis: analysis.detailedAnalysis,
                        
                        // Keep existing scoring for database
                        score: analysis.score,
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

        console.log(`   🎯 Found ${undervaluedSales.length} undervalued sales (Claude AI 10% threshold)`);
        return undervaluedSales;
    }

    /**
     * FIXED: Save undervalued sales to database with Claude AI analysis data
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
                                analysis_date: new Date().toISOString(),
                                reasoning: sale.reasoning // Update Claude reasoning
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

                // Enhanced database record with Claude AI analysis data
                const dbRecord = {
                    listing_id: sale.id?.toString(),
                    address: sale.address,
                    neighborhood: sale.neighborhood,
                    borough: sale.borough || 'unknown',
                    zipcode: sale.zipcode,
                    
                    // Advanced sales pricing analysis
                    price: parseInt(sale.salePrice) || 0,
                    price_per_sqft: sale.actualPrice && sale.sqft > 0 ? parseFloat((sale.actualPrice / sale.sqft).toFixed(2)) : null,
                    market_price_per_sqft: sale.estimatedMarketPrice && sale.sqft > 0 ? parseFloat((sale.estimatedMarketPrice / sale.sqft).toFixed(2)) : null,
                    discount_percent: parseFloat(sale.discountPercent.toFixed(2)),
                    potential_savings: parseInt(sale.potentialProfit) || 0,
                    
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
                    
                    // HOA and taxes
                    monthly_hoa: parseInt(sale.monthlyHoa) || 0,
                    monthly_tax: parseInt(sale.monthlyTax) || 0,
                    
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
                    
                    // CLAUDE AI ANALYSIS RESULTS
                    score: parseInt(sale.score) || 0,
                    grade: sale.grade || 'C',
                    deal_quality: this.calculateDealQuality(parseInt(sale.score) || 0),
                    reasoning: sale.reasoning || 'Claude AI market analysis',
                    comparison_group: sale.comparisonGroup || '',
                    comparison_method: sale.valuationMethod || sale.comparisonMethod || '',
                    reliability_score: parseInt(sale.confidence) || 0,
                    
                    // Enhanced Claude AI fields
                    detailed_analysis: typeof sale.detailedAnalysis === 'object' ? sale.detailedAnalysis : {},
                    
                    // Additional data
                    building_info: typeof sale.building === 'object' ? sale.building : {},
                    agents: Array.isArray(sale.agents) ? sale.agents : [],
                    sale_type: sale.type || 'sale',
                    
                    // Deduplication and sold tracking fields
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
                    console.log(`   ✅ Saved: ${sale.address} (${sale.discountPercent}% below market, Claude AI: "${sale.reasoning?.substring(0, 80)}...")`);
                    savedCount++;
                }
            } catch (error) {
                console.error(`   ❌ Error processing sale ${sale.address}:`, error.message);
            }
        }

        console.log(`   💾 Saved ${savedCount} new undervalued sales using Claude AI analysis`);
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
                query = query.lte('price', criteria.maxPrice);
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
            
            console.log('\n🚨 CRITICAL: You need to create the sales_market_cache table:');
            console.log(`
CREATE TABLE public.sales_market_cache (
    id uuid PRIMARY KEY DEFAULT extensions.uuid_generate_v4(),
    listing_id text NOT NULL UNIQUE,
    address text,
    neighborhood text,
    borough text,
    price bigint,
    bedrooms integer,
    bathrooms numeric(3,1),
    sqft integer,
    property_type text,
    market_status text DEFAULT 'pending',
    last_checked timestamp with time zone DEFAULT now(),
    last_seen_in_search timestamp with time zone DEFAULT now(),
    last_analyzed timestamp with time zone,
    times_seen integer DEFAULT 1,
    created_at timestamp with time zone DEFAULT now()
);

CREATE INDEX idx_sales_cache_listing_id ON public.sales_market_cache (listing_id);
CREATE INDEX idx_sales_cache_neighborhood ON public.sales_market_cache (neighborhood);
CREATE INDEX idx_sales_cache_last_seen ON public.sales_market_cache (last_seen_in_search DESC);
CREATE INDEX idx_sales_cache_market_status ON public.sales_market_cache (market_status);
            `);
            
        } catch (error) {
            console.error('❌ Sales database setup error:', error.message);
        }
    }
}

// CLI interface for sales with enhanced deduplication features and Claude AI valuation
async function main() {
    const args = process.argv.slice(2);
    
    if (args.includes('--help')) {
        console.log('🏠 FIXED Enhanced Claude AI Sales Analyzer');
        console.log('');
        console.log('Usage:');
        console.log('  node enhanced-biweekly-streeteasy-sales.js                    # Run bi-weekly analysis');
        console.log('  node enhanced-biweekly-streeteasy-sales.js --test soho       # Test single neighborhood');
        console.log('  node enhanced-biweekly-streeteasy-sales.js --latest 20       # Show latest deals');
        console.log('  node enhanced-biweekly-streeteasy-sales.js --top-deals 10    # Show top deals');
        console.log('  node enhanced-biweekly-streeteasy-sales.js --neighborhood park-slope  # Show deals by area');
        console.log('  node enhanced-biweekly-streeteasy-sales.js --doorman         # Show doorman building deals');
        console.log('  node enhanced-biweekly-streeteasy-sales.js --setup           # Show database setup');
        console.log('  node enhanced-biweekly-streeteasy-sales.js --help            # Show this help');
        console.log('');
        console.log('Environment Variables:');
        console.log('  TEST_NEIGHBORHOOD=soho          # Test single neighborhood');
        console.log('  INITIAL_BULK_LOAD=true          # Process all neighborhoods (first run)');
        console.log('  ANTHROPIC_API_KEY=sk-...        # Claude AI API key');
        console.log('  RAPIDAPI_KEY=...                # StreetEasy API key');
        console.log('  SUPABASE_URL=...                # Supabase project URL');
        console.log('  SUPABASE_ANON_KEY=...           # Supabase anon key');
        console.log('');
        console.log('FIXED Features:');
        console.log('  🤖 FIXED: Claude AI JSON parsing and null reference errors');
        console.log('  🎯 FIXED: Rate limiting matches rentals exactly (4-8s delays)');
        console.log('  💾 FIXED: Sold detection only affects current neighborhood');
        console.log('  🏠 FIXED: Hierarchical comparable filtering with error handling');
        console.log('  ⚡ FIXED: Adaptive rate limiting with proper delay logic');
        return;
    }
    
    if (!process.env.RAPIDAPI_KEY || !process.env.SUPABASE_URL || !process.env.SUPABASE_ANON_KEY || !process.env.ANTHROPIC_API_KEY) {
        console.error('❌ Missing required environment variables!');
        console.error('   RAPIDAPI_KEY, SUPABASE_URL, SUPABASE_ANON_KEY, ANTHROPIC_API_KEY required');
        console.error('\n💡 For testing, you can also set:');
        console.error('   TEST_NEIGHBORHOOD=soho (to test single neighborhood)');
        process.exit(1);
    }

    const analyzer = new EnhancedBiWeeklySalesAnalyzer();

    if (args.includes('--test')) {
        const neighborhood = args[args.indexOf('--test') + 1];
        if (!neighborhood) {
            console.error('❌ Please provide a neighborhood: --test park-slope');
            console.error('🧪 Valid examples: soho, east-village, west-village, williamsburg, park-slope');
            return;
        }
        
        console.log(`🧪 TESTING FIXED Claude AI Sales Analysis on: ${neighborhood}`);
        console.log('⚡ This will run full analysis with FIXED rate limiting and Claude JSON parsing');
        
        // Override environment for testing
        process.env.TEST_NEIGHBORHOOD = neighborhood;
        process.env.INITIAL_BULK_LOAD = 'false';
        
        const results = await analyzer.runBiWeeklySalesRefresh();
        
        console.log('\n🎉 FIXED Test completed! Check results above.');
        return results;
    }

    if (args.includes('--setup')) {
        await analyzer.setupSalesDatabase();
        return;
    }

    if (args.includes('--latest')) {
        const limit = parseInt(args[args.indexOf('--latest') + 1]) || 20;
        const sales = await analyzer.getLatestUndervaluedSales(limit);
        console.log(`🏠 Latest ${sales.length} active undervalued sales (FIXED Claude AI analysis):`);
        sales.forEach((sale, i) => {
            console.log(`${i + 1}. ${sale.address} - ${sale.price.toLocaleString()} (${sale.discount_percent}% below market, Score: ${sale.score})`);
            console.log(`   📝 Claude: "${sale.reasoning?.substring(0, 100)}..."`);
        });
        return;
    }

    if (args.includes('--top-deals')) {
        const limit = parseInt(args[args.indexOf('--top-deals') + 1]) || 10;
        const deals = await analyzer.getTopSaleDeals(limit);
        console.log(`🏆 Top ${deals.length} active sale deals (FIXED Claude AI analysis):`);
        deals.forEach((deal, i) => {
            console.log(`${i + 1}. ${deal.address} - ${deal.price.toLocaleString()} (${deal.discount_percent}% below market, Score: ${deal.score})`);
            console.log(`   📝 Claude: "${deal.reasoning?.substring(0, 100)}..."`);
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
        console.log(`🏠 Active sales in ${neighborhood} (FIXED Claude AI analysis):`);
        sales.forEach((sale, i) => {
            console.log(`${i + 1}. ${sale.address} - ${sale.price.toLocaleString()} (Score: ${sale.score})`);
        });
        return;
    }

    if (args.includes('--doorman')) {
        const sales = await analyzer.getSalesByCriteria({ doorman: true, limit: 15 });
        console.log(`🚪 Active doorman building sales (FIXED Claude AI analysis):`);
        sales.forEach((sale, i) => {
            console.log(`${i + 1}. ${sale.address} - ${sale.price.toLocaleString()} (${sale.discount_percent}% below market)`);
        });
        return;
    }

    // Default: run bi-weekly sales analysis with FIXED Claude AI
    console.log('🏠 Starting FIXED CLAUDE AI bi-weekly sales analysis...');
    console.log('🔧 FIXES: Rate limiting, JSON parsing, sold detection, null references');
    const results = await analyzer.runBiWeeklySalesRefresh();
    
    console.log('\n🎉 FIXED Claude AI sales analysis with smart deduplication completed!');
    
    if (results.summary && results.summary.apiCallsSaved > 0) {
        const efficiency = ((results.summary.apiCallsSaved / (results.summary.apiCallsUsed + results.summary.apiCallsSaved)) * 100).toFixed(1);
        console.log(`⚡ Achieved ${efficiency}% API efficiency through smart caching!`);
    }
    
    if (results.summary && results.summary.savedToDatabase) {
        console.log(`📊 Check your Supabase 'undervalued_sales' table for ${results.summary.savedToDatabase} new deals!`);
        console.log(`🤖 All properties include FIXED Claude AI natural language explanations`);
    }
    
    return results;
}

// Export for use in other modules
module.exports = EnhancedBiWeeklySalesAnalyzer;

// Run if executed directly
if (require.main === module) {
    main().catch(error => {
        console.error('💥 FIXED Enhanced Claude AI sales analyzer crashed:', error);
        process.exit(1);
    });
}
