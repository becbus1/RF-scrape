// claude-powered-sales-system.js
// COMPLETE SALES ADAPTATION - Comprehensive sales opportunity detection with Claude AI analysis
// ADAPTED FROM: Working claude-powered-rentals-system.js
// FEATURES:
// 1. Two-stage Claude analysis (quick check → detailed reasoning for undervalued only)
// 2. Consumer + Investor focused analysis
// 3. Smart caching to reduce API calls
// 4. Property type analysis (condo/co-op/townhouse)
// 5. HOA/tax factor integration
// 6. Dynamic undervaluation thresholds

require('dotenv').config();
const axios = require('axios');
const { createClient } = require('@supabase/supabase-js');
const EnhancedClaudeMarketAnalyzer = require('./claude-market-analyzer.js');

class ClaudePoweredSalesSystem {
    constructor() {
        // Supabase configuration
        this.supabase = createClient(
            process.env.SUPABASE_URL,
            process.env.SUPABASE_ANON_KEY
        );
        
        // API configuration
        this.rapidApiKey = process.env.RAPIDAPI_KEY;
        this.claudeAnalyzer = new EnhancedClaudeMarketAnalyzer();
        
        // Analysis thresholds - SALES SPECIFIC
        this.undervaluationThreshold = parseInt(process.env.SALES_UNDERVALUATION_THRESHOLD) || 15;
        this.lowInventoryThreshold = parseInt(process.env.SALES_LOW_INVENTORY_THRESHOLD) || 10;
        this.inventoryBreakpoint = parseInt(process.env.SALES_INVENTORY_BREAKPOINT) || 200;
        this.maxListingsPerNeighborhood = parseInt(process.env.MAX_SALES_PER_NEIGHBORHOOD) || 500;
        
        // Cache settings
        this.cacheDuration = parseInt(process.env.CACHE_DURATION_DAYS) || 7;
        
        // Statistics tracking
        this.apiCallsUsed = 0;
        this.totalAnalyzed = 0;
        this.undervaluedCount = 0;
        this.twoStageCallsSaved = 0;
        this.rentStabilizedBuildings = [];
        
        console.log('🏠 Claude-Powered Sales Opportunity System initialized');
        console.log(`   🎯 Base undervaluation threshold: ${this.undervaluationThreshold}%`);
        console.log(`   📊 Low inventory threshold: ${this.lowInventoryThreshold}% (for small neighborhoods)`);
        console.log(`   🔢 Inventory breakpoint: ${this.inventoryBreakpoint} listings`);
    }

    /**
     * MAIN ANALYSIS FUNCTION - Analyze neighborhood for sales opportunities
     */
    async analyzeNeighborhoodForSalesOpportunities(neighborhood, options = {}) {
        console.log(`\n🏘️ Analyzing ${neighborhood} for sales opportunities...`);
        
        const results = {
            neighborhood,
            totalListings: 0,
            totalAnalyzed: 0,
            undervaluedCount: 0,
            savedCount: 0,
            skippedCount: 0,
            quickChecks: 0,
            detailedAnalyses: 0,
            apiCallsSaved: 0,
            errors: []
        };
        
        try {
            // STEP 1: Fetch current active sales from API
            const activeListings = await this.fetchActiveSalesListings(neighborhood);
            console.log(`   🔍 Found ${activeListings.length} active sales`);
            
            if (activeListings.length === 0) {
                console.log(`   ⚠️ No active sales found for ${neighborhood}`);
                return results;
            }
            
            // STEP 2: Get cached listings for comparison
            const cachedListings = await this.getCachedNeighborhoodSales(neighborhood);
            console.log(`   📦 Found ${cachedListings.length} cached sales`);
            
            // STEP 3: Smart comparison - only analyze new/changed listings
            const { needFetch, priceUpdates, cacheHits } = await this.getSalesNeedingFetch(activeListings, cachedListings);
            console.log(`   🎯 Analysis needed: ${needFetch.length}, Cache hits: ${cacheHits}, Price changes: ${priceUpdates.length}`);
            
            // STEP 4: Mark missing listings as sold
            await this.markMissingListingsAsSold(activeListings.map(l => l.id), neighborhood);
            
            results.totalListings = activeListings.length;
            
            // STEP 5: Fetch detailed sales data FIRST
            console.log(`   🔍 Fetching detailed data for ${needFetch.length} properties...`);
            const detailedSales = await this.fetchDetailedSalesWithCache(needFetch.slice(0, this.maxListingsPerNeighborhood), neighborhood);
            console.log(`   ✅ Got detailed data for ${detailedSales.length} properties`);
            
            // STEP 6: TWO-STAGE CLAUDE ANALYSIS with dynamic threshold
            const analyzedProperties = [];
            const totalListings = results.totalListings || detailedSales.length;
            const dynamicThreshold = this.calculateDynamicThreshold(totalListings, neighborhood);

            // ✅ ADD: Load DHCR buildings for rent stabilization analysis  
            if (this.rentStabilizedBuildings.length === 0) {
                this.rentStabilizedBuildings = await this.loadRentStabilizedBuildings();
            }
            
            for (const listing of detailedSales) {
                try {
                    console.log(`🤖 Two-stage Claude analyzing sale: ${listing.address}`);
                    
                    // Skip if no address after detailed fetch
                    if (!listing.address || listing.address === 'Address not available') {
                        console.log(`     ⚠️ SKIPPED: No address for ${listing.id}`);
                        continue;
                    }
                    
                    // 🔧 MOVED: Cache ALL detailed listings immediately after detailed fetch
                    await this.cacheDetailedSalesListing(listing, neighborhood);

                       // ✅ ADD: Rent stabilization analysis for investor reasoning
                    const rentStabilizationAnalysis = this.claudeAnalyzer.generateRentStabilizationAnalysis(
                        listing, 
                        this.rentStabilizedBuildings
                    );
                    
                    // STAGE 1: Quick undervaluation check (no expensive reasoning)
                    const quickCheck = await this.claudeAnalyzer.analyzeSalesUndervaluation(
                        listing,
                        detailedSales,
                        neighborhood,
                        { 
                            undervaluationThreshold: dynamicThreshold,
                            skipDetailedReasoning: true
                        }
                    );
                    
                    results.quickChecks++;
                    
                    if (quickCheck && quickCheck.isUndervalued) {
                        console.log(`     📝 STAGE 2: Generating detailed analysis (${quickCheck.discountPercent?.toFixed(1)}% below market)`);
                        
                        // STAGE 2: Detailed consumer + investor analysis (only for undervalued)
                        const detailedAnalysis = await this.generateTwoTierSalesAnalysis(listing, quickCheck, detailedSales, neighborhood);
                        results.detailedAnalyses++;
                        
                        // Clean and prepare analysis data
                        const cleanAnalysis = this.cleanSalesAnalysisData(quickCheck, detailedAnalysis);
                        
                        const analyzedProperty = {
                            ...listing,
                            // Quick check results
                            percentBelowMarket: cleanAnalysis.discountPercent,
                            estimatedMarketPrice: cleanAnalysis.estimatedMarketPrice,
                            potentialSavings: cleanAnalysis.potentialSavings,
                            undervaluationConfidence: cleanAnalysis.confidence,
                            
                            // Detailed analysis results
                            consumerReasoning: cleanAnalysis.consumerReasoning,
                            investmentReasoning: cleanAnalysis.investmentReasoning,
                            propertyTypeAnalysis: cleanAnalysis.propertyTypeAnalysis,
                            hoaFactorAnalysis: cleanAnalysis.hoaFactorAnalysis,
                            rentStabilizationAnalysis: rentStabilizationAnalysis,

                            
                            // Classifications
                            isUndervalued: true,
                            
                            // Analysis metadata
                            analysisMethod: 'claude_two_stage',
                            reasoning: cleanAnalysis.consumerReasoning, // Primary reasoning
                            comparablesUsed: detailedSales.length,
                            fromCache: false
                        };
                        
                        analyzedProperties.push(analyzedProperty);
                        
                        // 🔧 REMOVED: No longer cache here - already cached above
                        
                        results.undervaluedCount++;
                        console.log(`     ✅ ${listing.address}: ${cleanAnalysis.discountPercent}% below market (${listing.propertyType})`);
                    } else {
                        console.log(`     ⚠️ Not undervalued: ${listing.address} (${quickCheck?.discountPercent?.toFixed(1) || 0}% < ${dynamicThreshold}%)`);
                        results.apiCallsSaved++; // Saved detailed analysis call
                    }
                    
                } catch (error) {
                    console.warn(`     ⚠️ Analysis exception for ${listing.address}: ${error.message}`);
                    results.errors.push(`${listing.address}: ${error.message}`);
                }
                
                results.totalAnalyzed++;
                
                // Rate limiting between properties
                await this.delay(100);
            }
            
            // Calculate API efficiency
            results.apiCallsSaved = results.quickChecks - results.detailedAnalyses;

   // 🔧 ADD THIS: Handle price updates for existing undervalued_sales
            if (priceUpdates.length > 0) {
                await this.handlePriceUpdatesForUndervaluedSales(priceUpdates);
            }
            
            // STEP 7: Save results to database
            if (analyzedProperties.length > 0) {
                await this.saveAnalyzedSalesProperties(analyzedProperties, neighborhood, results);
            }
            
            console.log(`   🎯 Two-stage efficiency: ${results.quickChecks} quick checks, ${results.detailedAnalyses} detailed analyses`);
            console.log(`   💰 API calls saved: ${results.apiCallsSaved} (${Math.round((results.apiCallsSaved / results.quickChecks) * 100)}% efficiency)`);
            
            return results;
            
        } catch (error) {
            console.error(`   ❌ Neighborhood analysis failed: ${error.message}`);
            results.errors.push(`Neighborhood analysis: ${error.message}`);
            throw error;
        }
    }

    /**
     * Get cached neighborhood sales from sales_market_cache
     */
    async getCachedNeighborhoodSales(neighborhood) {
        try {
            const { data, error } = await this.supabase
                .from('sales_market_cache')
                .select('*')
                .eq('neighborhood', neighborhood)
                .neq('market_status', 'fetch_failed')
                .not('address', 'is', null)
                .order('last_seen_in_search', { ascending: false })
                .limit(this.maxListingsPerNeighborhood);
            
            if (error) throw error;
            
            return (data || []).map(row => ({
                id: row.listing_id,
                address: row.address,
                price: row.sale_price || row.price,
                bedrooms: row.bedrooms,
                bathrooms: row.bathrooms,
                sqft: row.sqft,
                neighborhood: row.neighborhood,
                amenities: row.amenities || [],
                description: row.description || '',
                zipcode: row.zipcode,
                builtIn: row.built_in,
                propertyType: row.property_type || 'unknown',
                monthlyHoa: row.monthly_hoa,
                monthlyTax: row.monthly_tax
            }));
        } catch (error) {
            console.warn(`   ⚠️ Failed to get cached sales: ${error.message}`);
            return [];
        }
    }

    /**
     * Fetch active sales listings from StreetEasy API
     */
    async fetchActiveSalesListings(neighborhood) {
        try {
            console.log(`   🔍 Fetching active sales for ${neighborhood}...`);
            
            let allSales = [];
            let offset = 0;
            const limit = Math.min(500, this.maxListingsPerNeighborhood);
            const maxListings = this.maxListingsPerNeighborhood || 2000;
            let hasMoreData = true;
            
            while (hasMoreData && allSales.length < maxListings) {
                const response = await axios.get('https://streeteasy-api.p.rapidapi.com/sales/search', {
                    params: {
                        areas: neighborhood,
                        limit: limit,
                        minPrice: 100000,
                        maxPrice: 50000000,
                        offset: offset
                    },
                    headers: {
                        'X-RapidAPI-Key': this.rapidApiKey,
                        'X-RapidAPI-Host': 'streeteasy-api.p.rapidapi.com'
                    },
                    timeout: 30000
                });
                
                this.apiCallsUsed++;
                
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
                
                // Add to total
                allSales = allSales.concat(salesData);
                
                // Check if we should continue
                if (salesData.length < limit) {
                    hasMoreData = false;
                } else if (allSales.length >= maxListings) {
                    hasMoreData = false;
                } else {
                    offset += limit;
                }
            }
            
            console.log(`   ✅ Found ${allSales.length} active sales`);
            
            return allSales.map(listing => ({
                id: listing.id?.toString(),
                address: listing.address || 'Address not available',
                price: listing.price || 0,
                bedrooms: listing.bedrooms || 0,
                bathrooms: listing.bathrooms || 0,
                sqft: listing.sqft || 0,
                neighborhood: neighborhood,
                amenities: listing.amenities || [],
                description: listing.description || '',
                zipcode: listing.zipcode,
                builtIn: listing.builtIn,
                propertyType: listing.propertyType || 'unknown',
                url: listing.url || `https://streeteasy.com/sale/${listing.id}`,
                latitude: listing.latitude,
                longitude: listing.longitude
            }));
            
        } catch (error) {
            console.warn(`   ⚠️ Failed to fetch active sales: ${error.message}`);
            if (error.response) {
                console.warn(`   📊 Response status: ${error.response.status}`);
            }
            return [];
        }
    }

    /**
     * Clean Claude sales analysis data and convert to proper types
     */
    cleanSalesAnalysisData(quickCheck, detailedAnalysis) {
        return {
            discountPercent: this.safeDecimal(quickCheck.discountPercent, 1, 0),
            estimatedMarketPrice: this.safeInt(quickCheck.estimatedMarketPrice, 0),
            potentialSavings: this.safeInt(quickCheck.potentialSavings || quickCheck.potentialProfit, 0),
            confidence: this.safeInt(quickCheck.confidence || 0, 0),
            consumerReasoning: detailedAnalysis?.consumerReasoning || 'Property offers good value below market rate',
            investmentReasoning: detailedAnalysis?.investmentReasoning || 'Investment analysis not available',
            propertyTypeAnalysis: detailedAnalysis?.propertyTypeAnalysis || 'Standard property analysis',
            hoaFactorAnalysis: detailedAnalysis?.hoaFactorAnalysis || 'HOA fees within market range'
        };
    }

    /**
 * Generate two-tier sales analysis (Consumer + Investor focused) - UPDATED IMPLEMENTATION
 */
async generateTwoTierSalesAnalysis(listing, quickCheck, comparables, neighborhood) {
    try {

// ✅ FIXED: Generate rent stabilization analysis inside this function
        const rentStabilizationAnalysis = this.claudeAnalyzer.generateRentStabilizationAnalysis(
            listing, 
            this.rentStabilizedBuildings
        );

        // Single Claude call with dual analysis request
        const dualAnalysisPrompt = this.buildDualAnalysisPrompt(listing, quickCheck, neighborhood, rentStabilizationAnalysis);
        
        // Use Claude analyzer's callClaude method directly
        const response = await this.claudeAnalyzer.callClaude(
            this.buildDualAnalysisSystemPrompt(),
            dualAnalysisPrompt,
            'dual_sales_analysis'
        );
        
        if (response.success && response.analysis) {
            return {
                consumerReasoning: response.analysis.consumerReasoning || this.generateFallbackConsumerAnalysis(listing, quickCheck),
                investmentReasoning: response.analysis.investmentReasoning || this.generateFallbackInvestmentAnalysis(listing, quickCheck)
            };
        }
        
        // Fallback if Claude call fails
        return {
            consumerReasoning: this.generateFallbackConsumerAnalysis(listing, quickCheck),
            investmentReasoning: this.generateFallbackInvestmentAnalysis(listing, quickCheck)
        };
        
    } catch (error) {
        console.warn(`   ⚠️ Error generating dual analysis: ${error.message}`);
        return {
            consumerReasoning: this.generateFallbackConsumerAnalysis(listing, quickCheck),
            investmentReasoning: this.generateFallbackInvestmentAnalysis(listing, quickCheck)
        };
    }
}

/**
 * Build system prompt for dual consumer + investor analysis
 */
buildDualAnalysisSystemPrompt() {
    return `You are an expert NYC real estate analyst providing both consumer and investor perspectives on property deals.

Generate TWO distinct analyses for this undervalued property:

**CONSUMER ANALYSIS REQUIREMENTS:**
- Start with property type and location 
- State the exact discount percentage below market
- Mention the actual price vs estimated market price
- Explain why it's a good deal (features, location, value)
- Include new lower average monthly mortgage payment amount (not monthly savings)
- Note any reasons why it may be undervalued (if applicable)
- Keep to 3-4 sentences, conversational tone

**INVESTOR ANALYSIS REQUIREMENTS:**
You are an expert NYC residential real estate investor providing specific financial calculations. Write a concise 3-5 sentence investment memo with actionable numbers.

Must include:
1. **Specific Financial Metrics** – State exact cap rate, cash-on-cash return, and GRM with precision
2. **Monthly Cash Flow Calculation** – Calculate exact monthly cash flow after mortgage, taxes, HOA, maintenance, vacancy
3. **Break-Even Analysis** – What rent is needed to break even? How does this compare to estimated market rent?
4. **Equity Capture** – Quantify immediate equity gain from below-market purchase
5. **Value-Add Scenarios** – If renovation potential exists, provide specific cost estimates and projected ROI
6. **Deal Classification** – Classify as "Strong Buy," "Marginal," or "Pass" with specific reasoning

**RENT STABILIZATION RISK ASSESSMENT RULES:**
- If rent stabilization probability < 60%: Simply state "Property is almost certainly not rent-stabilized" and move on
- If rent stabilization probability ≥ 60%: Explain "High rent stabilization risk due to [specific factors: mention DHCR database match if present, building constructed in 19XX (rent stabilization era), estimated X+ units meeting 6+ unit threshold]. This caps rent increases to 2.5% annually vs 5-6% market rate, reducing 10-year income potential by approximately $X."

**TONE REQUIREMENTS:**
- NO generic phrases like "could be a solid BRRRR opportunity" 
- Lead with specific numbers and calculations
- Use precise investment terminology
- Focus on actionable financial data investors need

RESPONSE FORMAT (JSON):
{
  "consumerReasoning": "[Consumer analysis following the structure above]",
  "investmentReasoning": "[Investment memo with specific calculations and metrics]"
}

Provide factual, actionable, insightful analysis for both perspectives. Provide precise, calculation-heavy analysis that saves investors time on due diligence math for investment reasoning specifically.`;
}

/**
 * Build prompt context for dual analysis
 */
buildDualAnalysisPrompt(listing, quickCheck, neighborhood, rentStabilizationAnalysis) {
    const savings = quickCheck.potentialSavings || quickCheck.potentialProfit || 0;
    const annualSavings = savings; // One-time savings for sales
    const discountPercent = quickCheck.discountPercent || 0;
    const estimatedMarketPrice = quickCheck.estimatedMarketPrice || listing.price;
    const pricePerSqft = listing.sqft > 0 ? Math.round(listing.price / listing.sqft) : 0;
    const marketPricePerSqft = listing.sqft > 0 ? Math.round(estimatedMarketPrice / listing.sqft) : 0;
    
    // Calculate investor metrics
    const monthlyHoa = listing.monthlyHoa || 0;
    const monthlyTax = listing.monthlyTax || 0;
    const annualHoa = monthlyHoa * 12;
    const annualTax = monthlyTax * 12;
    
    // Estimate market rent (rough calculation: 0.8-1.2% of price monthly)
    const estimatedMonthlyRent = Math.round(listing.price * 0.01); // 1% rule
    const annualRent = estimatedMonthlyRent * 12;
    
    return `PROPERTY DETAILS:
Address: ${listing.address}
Neighborhood: ${neighborhood}
Property Type: ${listing.propertyType || 'Unknown'}
Purchase Price: $${listing.price?.toLocaleString()}
Estimated Market Value: $${estimatedMarketPrice.toLocaleString()}
Discount: ${discountPercent.toFixed(1)}% below market
Potential Savings: $${savings.toLocaleString()}

PROPERTY SPECIFICATIONS:
Bedrooms: ${listing.bedrooms}
Bathrooms: ${listing.bathrooms}
Square Feet: ${listing.sqft > 0 ? listing.sqft.toLocaleString() : 'Not specified'}
Price per sqft: $${pricePerSqft}
Market price per sqft: $${marketPricePerSqft}
Building Year: ${listing.builtIn || 'Not specified'}
Days on Market: ${listing.daysOnMarket || 'Not specified'}

FINANCIAL DETAILS:
Monthly HOA: $${monthlyHoa.toLocaleString()}
Annual HOA: $${annualHoa.toLocaleString()}
Monthly Taxes: $${monthlyTax.toLocaleString()}
Annual Taxes: $${annualTax.toLocaleString()}
Estimated Monthly Rent: $${estimatedMonthlyRent.toLocaleString()}
Estimated Annual Rent: $${annualRent.toLocaleString()}

AMENITIES & FEATURES:
${Array.isArray(listing.amenities) ? listing.amenities.slice(0, 10).join(', ') : 'Standard amenities'}

DESCRIPTION EXCERPT:
${typeof listing.description === 'string' ? listing.description.substring(0, 300) : 'No description available'}

INVESTMENT CALCULATION ASSUMPTIONS:
- Down Payment: 25% (${Math.round(listing.price * 0.25).toLocaleString()})
- Loan Amount: 75% (${Math.round(listing.price * 0.75).toLocaleString()})
- Interest Rate: 7%
- Loan Term: 30 years
- Vacancy Rate: 5%
- Maintenance: 8% of gross rent annually

RENT STABILIZATION ANALYSIS:
DHCR Database Match: ${rentStabilizationAnalysis?.dhcr_matches?.length > 0 ? 'YES' : 'NO'}
Rent Stabilization Probability: ${rentStabilizationAnalysis?.confidence_percentage || 0}%
Building Age: ${listing.builtIn || 'Unknown'} (Built ${listing.builtIn ? (listing.builtIn >= 1943 && listing.builtIn <= 1972 ? 'during rent stabilization era' : 'outside primary stabilization era') : 'unknown'})
Risk Assessment: ${rentStabilizationAnalysis?.confidence_percentage >= 60 ? 'HIGH RISK - Likely rent stabilized' : 'LOW RISK - Probably market rate'}

Generate both consumer and investment analyses for this ${discountPercent.toFixed(1)}% below-market ${listing.propertyType} in ${neighborhood}.`;
}

/**
 * Generate fallback consumer analysis (if Claude fails)
 */
generateFallbackConsumerAnalysis(listing, quickCheck) {
    const savings = quickCheck.potentialSavings || quickCheck.potentialProfit || 0;
    const discountPercent = quickCheck.discountPercent || 0;
    const estimatedMarketPrice = quickCheck.estimatedMarketPrice || listing.price;
    const propertyType = listing.propertyType || 'property';
    const neighborhood = listing.neighborhood || 'the area';
    
    return `This ${listing.bedrooms}-bedroom ${propertyType} in ${neighborhood} offers excellent value at $${listing.price?.toLocaleString()}, which is ${discountPercent.toFixed(1)}% below the estimated market value of $${estimatedMarketPrice.toLocaleString()}. ` +
           `The ${listing.sqft ? `${listing.sqft.toLocaleString()} square foot ` : ''}unit provides ${listing.bedrooms} bedrooms and ${listing.bathrooms} bathrooms in a desirable location. ` +
           `You'd be saving approximately $${savings.toLocaleString()} compared to similar properties in the area, making this a compelling opportunity for buyers seeking value in the ${neighborhood} market.`;
}

/**
 * Generate fallback investment analysis (if Claude fails)
 */
generateFallbackInvestmentAnalysis(listing, quickCheck) {
    const price = listing.price || 0;
    const monthlyHoa = listing.monthlyHoa || 0;
    const monthlyTax = listing.monthlyTax || 0;
    const estimatedMonthlyRent = Math.round(price * 0.01); // 1% rule
    const annualRent = estimatedMonthlyRent * 12;
    const grm = price > 0 ? (price / annualRent).toFixed(1) : 'N/A';
    const discountPercent = quickCheck.discountPercent || 0;
    
    return `Investment analysis: Property priced at ${discountPercent.toFixed(1)}% below market suggests immediate equity capture potential. ` +
           `Based on 1% rule, estimated monthly rent of $${estimatedMonthlyRent.toLocaleString()} yields GRM of ${grm}. ` +
           `Monthly carrying costs include $${monthlyHoa.toLocaleString()} HOA and $${monthlyTax.toLocaleString()} taxes. ` +
           `The below-market entry point provides cushion for potential rental income, though detailed cash flow analysis recommended. ` +
           `Consider neighborhood rental demand and comparable rent rolls before proceeding.`;
}

    /**
     * Build detailed analysis context for Claude
     */
    buildDetailedSalesAnalysisContext(listing, quickCheck, comparables, neighborhood) {
        return {
            listing,
            quickCheck,
            comparables: comparables.slice(0, 20), // Limit comparables for context
            neighborhood,
            analysisType: 'two_tier_sales',
            includeConsumerAnalysis: true,
            includeInvestmentAnalysis: true,
            includePropertyTypeAnalysis: true,
            includeHoaAnalysis: true
        };
    }

    /**
     * Analyze property type vs comparables
     */
    analyzePropertyType(listing, comparables) {
        const propertyType = listing.propertyType || 'unknown';
        const sameTypeComparables = comparables.filter(c => c.propertyType === propertyType);
        
        if (sameTypeComparables.length > 0) {
            const avgPrice = sameTypeComparables.reduce((sum, c) => sum + (c.price || 0), 0) / sameTypeComparables.length;
            const comparison = listing.price < avgPrice ? 'below' : 'above';
            return `${propertyType} analysis: This property is priced ${comparison} the average for similar ${propertyType}s in the area ($${avgPrice.toLocaleString()} average). ` +
                   `${sameTypeComparables.length} comparable ${propertyType}s were analyzed.`;
        }
        
        return `${propertyType} analysis: Limited comparable ${propertyType} data available for detailed comparison.`;
    }

    /**
     * Analyze HOA factor impact
     */
    analyzeHoaFactor(listing, quickCheck) {
        if (!listing.monthlyHoa || listing.monthlyHoa === 0) {
            return 'No HOA fees - reduces ongoing ownership costs and increases net savings potential.';
        }
        
        const annualHoa = listing.monthlyHoa * 12;
        const hoaImpact = listing.price > 0 ? (annualHoa / listing.price * 100).toFixed(2) : 0;
        
        return `Monthly HOA of $${listing.monthlyHoa?.toLocaleString()} ($${annualHoa.toLocaleString()}/year) represents ${hoaImpact}% of purchase price annually. ` +
               `Factor this into total cost of ownership when evaluating the ${quickCheck.discountPercent?.toFixed(1)}% market discount.`;
    }

    /**
     * Fetch detailed property information with caching
     */
    async fetchDetailedSalesWithCache(listings, neighborhood) {
        const detailed = [];
        
        for (const listing of listings) {
            try {
                this.apiCallsUsed++;
                
                const response = await axios.get(`https://streeteasy-api.p.rapidapi.com/sales/${listing.id}`, {
                    headers: {
                        'X-RapidAPI-Key': this.rapidApiKey,
                        'X-RapidAPI-Host': 'streeteasy-api.p.rapidapi.com'
                    },
                    timeout: 15000
                });
                
                if (response.data) {
                    const detailedListing = {
                        ...listing,
                        ...response.data,
                        neighborhood: neighborhood,
                        // Ensure we have the complete StreetEasy sales data
                        listedAt: response.data.listedAt,
                        daysOnMarket: response.data.daysOnMarket,
                        ppsqft: response.data.ppsqft,
                        monthlyHoa: response.data.monthlyHoa,
                        monthlyTax: response.data.monthlyTax,
                        building: response.data.building,
                        agents: response.data.agents,
                        images: response.data.images,
                        videos: response.data.videos,
                        floorplans: response.data.floorplans
                    };
                    
                    detailed.push(detailedListing);
                }
                
            } catch (error) {
                if (error.response?.status === 429) {
                    console.warn(`     ⚠️ Rate limit hit for sale ${listing.id}`);
                    await this.delay(2000);
                } else {
                    console.warn(`     ⚠️ Failed to fetch details for ${listing.id}: ${error.message}`);
                }
            }
            
            await this.delay(200); // Rate limiting
        }
        
        return detailed;
    }

    /**
     * Cache detailed sales listing information
     */
    async cacheDetailedSalesListing(listing, neighborhood) {
        // Only cache if fully detailed
        if (!listing.address || listing.bedrooms === undefined) {
            console.warn(`     ⚠️ Not caching incomplete listing ${listing.id}`);
            return;
        }
        
        try {
            const cacheData = {
                listing_id: listing.id?.toString(),
                address: listing.address,
                sale_price: listing.price,
                price: listing.price, // Both fields for compatibility
                bedrooms: listing.bedrooms,
                bathrooms: listing.bathrooms,
                sqft: listing.sqft || 0,
                neighborhood: neighborhood,
                borough: this.getBoroughFromNeighborhood(neighborhood),
                property_type: listing.propertyType || 'unknown',
                amenities: listing.amenities || [],
                description: listing.description || '',
                zipcode: listing.zipcode,
                built_in: listing.builtIn,
                monthly_hoa: listing.monthlyHoa,
                monthly_tax: listing.monthlyTax,
                ppsqft: listing.ppsqft,
                latitude: listing.latitude,
                longitude: listing.longitude,
                images: listing.images || [],
                agents: listing.agents || [],
                building_info: listing.building || {},
                market_status: 'pending',
                last_seen_in_search: new Date().toISOString(),
                last_checked: new Date().toISOString(),
                times_seen: 1
            };
                
            const { error } = await this.supabase
                .from('sales_market_cache')
                .upsert(cacheData, { onConflict: 'listing_id' });
            
            if (error) {
                console.warn(`     ⚠️ Failed to cache sales listing: ${error.message}`);
            }
        } catch (error) {
            console.warn(`     ⚠️ Exception caching sales listing: ${error.message}`);
        }
    }

/**
     * Update cache with analysis results (mark as undervalued or market_rate)
     */
    async updateCacheWithSalesAnalysisResults(detailedSales, undervaluedSales) {
        try {
            const cacheUpdates = detailedSales.map(sale => {
                const isUndervalued = undervaluedSales.some(us => us.id === sale.id);
                
                return {
                    listing_id: sale.id?.toString(),
                    market_status: isUndervalued ? 'undervalued' : 'market_rate',
                    last_analyzed: new Date().toISOString(),
                    last_checked: new Date().toISOString()
                };
            });

            // Batch update all analyzed properties
            for (const update of cacheUpdates) {
                const { error } = await this.supabase
                    .from('sales_market_cache')
                    .update({
                        market_status: update.market_status,
                        last_analyzed: update.last_analyzed,
                        last_checked: update.last_checked
                    })
                    .eq('listing_id', update.listing_id);

                if (error) {
                    console.warn(`⚠️ Failed to update cache for ${update.listing_id}:`, error.message);
                }
            }

            console.log(`   💾 Updated cache for ${cacheUpdates.length} analyzed properties`);
            
        } catch (error) {
            console.warn(`⚠️ Error updating cache with analysis results:`, error.message);
        }
    }
    
   /**
     * Smart caching - check which sales need individual API calls
     */
    async getSalesNeedingFetch(activeListings, cachedListings) {
        const needFetch = [];
        const priceUpdates = [];
        
        for (const listing of activeListings) {
            // 🔧 FIX 1: Use listing_id field (not id)
            const cached = cachedListings.find(c => c.listing_id === listing.id?.toString());
            
            if (!cached) {
                // New listing - needs full fetch
                needFetch.push(listing);
            // 🔧 FIX 2: Use correct price field names 
            } else if (cached.price !== listing.price) {
                // Price changed - needs re-fetch and analysis update
                console.log(`   💰 Price change detected for ${listing.id}: ${cached.price} → ${listing.price}`);
                needFetch.push(listing);
                priceUpdates.push({ id: listing.id, oldPrice: cached.price, newPrice: listing.price });
            // 🔧 FIX 3: Add market_status check to skip already analyzed
            } else if (cached.market_status === 'pending' || cached.market_status === 'fetch_failed') {
                // Never analyzed or failed - needs analysis
                needFetch.push(listing);
            }
            // else: cached, same price, and already analyzed - skip
        }
        
        return { needFetch, priceUpdates, cacheHits: activeListings.length - needFetch.length };
    }

   /**
     * Mark missing listings as likely sold and handle price updates
     */
    async markMissingListingsAsSold(currentListingIds, neighborhood) {
        try {
            const currentIds = new Set(currentListingIds.map(id => id.toString()));
            
            // Find cached listings not in current search
            const { data: cachedListings, error } = await this.supabase
                .from('sales_market_cache')
                .select('listing_id')
                .eq('neighborhood', neighborhood)
                .not('address', 'is', null)
                .neq('market_status', 'likely_sold');
            
            if (error) throw error;
            
            const missingIds = (cachedListings || [])
                .map(row => row.listing_id)
                .filter(id => !currentIds.has(id));
            
            if (missingIds.length > 0) {
                // Mark as likely sold in cache
                const { error: updateError } = await this.supabase
                    .from('sales_market_cache')
                    .update({ 
                        market_status: 'likely_sold',
                        last_checked: new Date().toISOString()
                    })
                    .in('listing_id', missingIds);
                
                if (updateError) throw updateError;

                // Mark corresponding entries in undervalued_sales as likely sold
                const { error: markSalesError } = await this.supabase
                    .from('undervalued_sales')
                    .update({
                        status: 'likely_sold',
                        likely_sold: true,
                        sold_detected_at: new Date().toISOString()
                    })
                    .in('listing_id', missingIds)
                    .eq('status', 'active');
                
                console.log(`   🔄 Marked ${missingIds.length} missing listings as likely sold`);
                
                if (markSalesError) {
                    console.warn('⚠️ Error marking undervalued_sales as sold:', markSalesError.message);
                }
            }
            
        } catch (error) {
            console.warn(`   ⚠️ Error marking missing listings: ${error.message}`);
        }
    }

    /**
     * Load rent-stabilized buildings from Supabase (CRITICAL for accuracy)
     */
    async loadRentStabilizedBuildings() {
        try {
            let allBuildings = [];
            let offset = 0;
            const batchSize = 1000;
            let hasMoreData = true;
            
            console.log('🏢 Loading rent-stabilized buildings from DHCR data...');
            
            while (hasMoreData) {
                console.log(`   📊 Loading batch starting at offset ${offset}...`);
                
                const { data, error } = await this.supabase
                    .from('rent_stabilized_buildings')
                    .select('*')
                    .range(offset, offset + batchSize - 1)
                    .order('id');
                
                if (error) {
                    console.error(`   ❌ Error loading buildings at offset ${offset}:`, error.message);
                    throw error;
                }
                
                // Robust stopping condition
                if (!data || data.length === 0) {
                    hasMoreData = false;
                    break;
                }
                
                allBuildings = allBuildings.concat(data);
                console.log(`   ✅ Loaded ${data.length} buildings (total: ${allBuildings.length})`);
                
                // Dynamic continuation
                if (data.length < batchSize) {
                    hasMoreData = false;
                }
                
                offset += batchSize;
                
                // Safety check
                if (offset > 100000) {
                    console.log('   ⚠️ Reached safety limit of 100,000 buildings');
                    hasMoreData = false;
                }
            }
            
            console.log(`   ✅ Loaded ${allBuildings.length} total rent-stabilized buildings`);
            return allBuildings;
            
        } catch (error) {
            console.error('Failed to load rent-stabilized buildings:', error.message);
            console.log('   🔄 Continuing without DHCR data - will use building characteristics only');
            return [];
        }
    }
    
    /**
     * Handle price change updates for undervalued_sales table
     */
    async handlePriceUpdatesForUndervaluedSales(priceUpdates) {
        if (priceUpdates.length === 0) return;
        
        try {
            for (const update of priceUpdates) {
                // Update price in undervalued_sales if it exists
                const { error } = await this.supabase
                    .from('undervalued_sales')
                    .update({
                        price: update.newPrice,
                        last_seen_in_search: new Date().toISOString()
                    })
                    .eq('listing_id', update.id.toString())
                    .eq('status', 'active');
                
                if (error) {
                    console.warn(`⚠️ Error updating price for undervalued sale ${update.id}:`, error.message);
                } else {
                    console.log(`   💰 Updated undervalued_sales price for ${update.id}: ${update.oldPrice} → ${update.newPrice}`);
                }
            }
        } catch (error) {
            console.warn(`⚠️ Error handling price updates for undervalued_sales:`, error.message);
        }
    }

    /**
     * Remove likely sold properties from analysis tables
     */
    async removeSoldFromAnalysisTables(listingIds) {
        try {
            // Remove from undervalued_sales table
            await this.supabase.from('undervalued_sales').delete().in('listing_id', listingIds);
            
            console.log(`   🗑️ Removed ${listingIds.length} sold properties from analysis tables`);
        } catch (error) {
            console.warn(`   ⚠️ Error removing sold properties: ${error.message}`);
        }
    }

    /**
     * Handle price change updates (re-analyze and update tables)
     */
    async handlePriceChangeUpdates(priceUpdates, analyzedProperties) {
        for (const update of priceUpdates) {
            try {
                // Find the re-analyzed property
                const property = analyzedProperties.find(p => p.id === update.id);
                if (!property) continue;
                
                console.log(`   🔄 Updating analysis for ${property.address} (price: ${update.oldPrice} → ${update.newPrice})`);
                
                // Update cache with new price
                await this.supabase
                    .from('sales_market_cache')
                    .update({ 
                        sale_price: update.newPrice,
                        price: update.newPrice,
                        last_checked: new Date().toISOString() 
                    })
                    .eq('listing_id', update.id);
                
                // Remove old analysis (if exists) and re-save with new analysis
                await this.supabase.from('undervalued_sales').delete().eq('listing_id', update.id);
                
                // Re-save will happen in normal save flow with updated analysis
                
            } catch (error) {
                console.warn(`   ⚠️ Error handling price update for ${update.id}: ${error.message}`);
            }
        }
    }

    /**
     * Calculate dynamic undervaluation threshold based on neighborhood size
     */
    calculateDynamicThreshold(activeListingsCount, neighborhood) {
        const baseThreshold = this.undervaluationThreshold; // 15% from env
        const lowInventoryThreshold = this.lowInventoryThreshold; // 10% from env
        const inventoryBreakpoint = this.inventoryBreakpoint; // 200 from env
        
        if (activeListingsCount < inventoryBreakpoint) {
            console.log(`   📊 Small neighborhood (${activeListingsCount} listings) - Using ${lowInventoryThreshold}% threshold for variety`);
            return lowInventoryThreshold;
        } else {
            console.log(`   📊 Large neighborhood (${activeListingsCount} listings) - Using ${baseThreshold}% threshold for quality`);
            return baseThreshold;
        }
    }

    /**
     * Save analyzed sales properties to database
     */
    async saveAnalyzedSalesProperties(properties, neighborhood, results) {
        // Calculate dynamic threshold based on neighborhood size
        const totalListings = results.totalListings || properties.length;
        const dynamicUndervaluationThreshold = this.calculateDynamicThreshold(totalListings, neighborhood);
        
        console.log(`   💾 Saving ${properties.length} analyzed sales using dynamic thresholds...`);
        console.log(`   📊 Using ${dynamicUndervaluationThreshold}% undervaluation threshold for this neighborhood`);
        
        let savedCount = 0;
        let skipped = 0;
        
        for (const property of properties) {
            try {
                // Check if meets undervaluation threshold
                const isUndervalued = property.percentBelowMarket >= dynamicUndervaluationThreshold;
                
                if (isUndervalued) {
                    // Save to undervalued_sales table
                    await this.saveToUndervaluedSalesTable(property, neighborhood);
                    savedCount++;
                    console.log(`     💰 SAVED: ${property.address} (${property.percentBelowMarket?.toFixed(1)}% below market, ${property.propertyType})`);
                } else {
                    skipped++;
                    console.log(`     ⚠️ SKIPPED: ${property.address} - Not undervalued enough (${property.percentBelowMarket?.toFixed(1)}% < ${dynamicUndervaluationThreshold}%)`);
                }
                
            } catch (error) {
                console.error(`     ❌ Exception saving ${property.address}: ${error.message}`);
                skipped++;
            }
        }
        
        console.log(`   📊 Save Summary: ${savedCount} saved, ${skipped} skipped`);
        
        // Update results
        results.savedCount = savedCount;
        results.skippedCount = skipped;
    }

    /**
     * Save to undervalued_sales table with all sales-specific fields
     */
    async saveToUndervaluedSalesTable(property, neighborhood) {
        try {
            const saveData = {
                listing_id: property.id?.toString(),
                listing_url: property.url || `https://streeteasy.com/sale/${property.id}`,
                address: property.address,
                neighborhood: neighborhood,
                borough: this.getBoroughFromNeighborhood(neighborhood),
                zipcode: property.zipcode,
                
                // Pricing analysis - SALES SPECIFIC
                price: this.safeInt(property.price),
                estimated_market_price: this.safeInt(property.estimatedMarketPrice),
                discount_percent: this.safeDecimal(property.percentBelowMarket, 2, 0),
                potential_savings: this.safeInt(property.potentialSavings, 0),
                price_per_sqft: property.sqft > 0 ? this.safeDecimal(property.price / property.sqft, 2) : null,
                market_price_per_sqft: (property.estimatedMarketPrice && property.sqft > 0) ? 
                    this.safeDecimal(property.estimatedMarketPrice / property.sqft, 2) : null,
                ppsqft: this.safeDecimal(property.ppsqft, 2, null),
                
                // Property details
                bedrooms: this.safeInt(property.bedrooms, 0),
                bathrooms: this.safeDecimal(property.bathrooms, 1, 0),
                sqft: this.safeInt(property.sqft, null),
                property_type: property.propertyType || 'unknown',
                
                // Sales-specific fields
                listed_at: property.listedAt ? new Date(property.listedAt).toISOString() : null,
                days_on_market: this.safeInt(property.daysOnMarket, 0),
                monthly_hoa: this.safeDecimal(property.monthlyHoa, 2, null),
                monthly_tax: this.safeDecimal(property.monthlyTax, 2, null),
                
                // Building info
                built_in: this.safeInt(property.builtIn, null),
                latitude: this.safeDecimal(property.latitude, 8, null),
                longitude: this.safeDecimal(property.longitude, 8, null),
                building_id: property.building?.id?.toString() || null,
                
                // Media and description
                images: this.validateJsonField(property.images, []),
                image_count: this.safeInt((property.images || []).length),
                videos: this.validateJsonField(property.videos, []),
                floorplans: this.validateJsonField(property.floorplans, []),
                description: property.description || '',
                amenities: this.validateTextArrayField(property.amenities),
                amenity_count: this.safeInt((property.amenities || []).length),
                
                // Analysis results - DUAL REASONING
                score: this.safeInt(this.calculateSalesPropertyScore(property)),
                grade: this.calculatePropertyGrade(this.calculateSalesPropertyScore(property)),
                deal_quality: this.safeDealQuality(this.calculateSalesPropertyScore(property)),
                reasoning: property.consumerReasoning || property.reasoning || 'AI-powered sales analysis',
                investment_reasoning: property.investmentReasoning || 'Investment analysis performed',
                consumer_reasoning: property.consumerReasoning || property.reasoning || 'Consumer analysis performed',
                comparison_group: `${property.bedrooms}BR/${property.bathrooms}BA ${property.propertyType}s in ${neighborhood}`,
                comparison_method: property.analysisMethod || 'claude_two_stage',
                reliability_score: this.safeInt(property.undervaluationConfidence, 0),
                
                // Additional fields
                building_info: this.validateJsonField(property.building, {}),
                agents: this.validateJsonField(property.agents, []),
                
                // Metadata
                analysis_date: new Date().toISOString(),
                status: 'active',
                last_seen_in_search: new Date().toISOString(),
                times_seen_in_search: 1,
                likely_sold: false,
                undervaluation_category: 'claude_two_stage',
                undervaluation_phrases: [],
                category_confidence: this.safeInt(property.undervaluationConfidence, 0)
            };
            
            const { error } = await this.supabase
                .from('undervalued_sales')
                .upsert(saveData, { onConflict: 'listing_id' });
            
            if (error) {
                console.error(`     ❌ Sales save error for ${property.address}: ${error.message}`);
                throw error;
            }
            
        } catch (error) {
            console.error(`     ❌ ERROR saving sales property: ${error.message}`);
            throw error;
        }
    }

    /**
     * Validate JSON field
     */
    validateJsonField(field, defaultValue) {
        try {
            if (field === null || field === undefined) {
                return defaultValue;
            }
            if (typeof field === 'string') {
                return JSON.parse(field);
            }
            if (typeof field === 'object') {
                return field;
            }
            return defaultValue;
        } catch (error) {
            console.warn(`⚠️ JSON field validation error: ${error.message}`);
            return defaultValue;
        }
    }

    /**
     * Validate text array field (for amenities)
     */
    validateTextArrayField(field) {
        try {
            if (field === null || field === undefined) return [];
            if (typeof field === 'string') {
                try {
                    const parsed = JSON.parse(field);
                    if (Array.isArray(parsed)) {
                        return parsed.map(item => String(item));
                    }
                } catch (parseError) {
                    return [field];
                }
            }
            if (Array.isArray(field)) {
                return field.map(item => String(item));
            }
            return [];
        } catch (error) {
            console.warn(`⚠️ Text array validation error: ${error.message}`);
            return [];
        }
    }

    /**
     * Check if property has specific amenities
     */
    hasAmenity(amenities, searchTerms) {
        if (!Array.isArray(amenities)) return false;
        
        return searchTerms.some(term => 
            amenities.some(amenity => 
                amenity.toLowerCase().includes(term.toLowerCase())
            )
        );
    }

    /**
     * BULLETPROOF: Convert any value to integer safely
     */
    safeInt(value, defaultValue = 0) {
        if (value === null || value === undefined) return defaultValue;
        const num = parseFloat(value);
        return isNaN(num) ? defaultValue : Math.round(num);
    }

    /**
     * BULLETPROOF: Convert any value to decimal safely
     */
    safeDecimal(value, decimals = 2, defaultValue = 0) {
        if (value === null || value === undefined) return defaultValue;
        const num = parseFloat(value);
        return isNaN(num) ? defaultValue : parseFloat(num.toFixed(decimals));
    }

    /**
     * BULLETPROOF: Ensure deal quality is valid constraint value
     */
    safeDealQuality(score) {
        const intScore = this.safeInt(score, 50);
        if (intScore >= 90) return 'best';
        if (intScore >= 80) return 'excellent'; 
        if (intScore >= 70) return 'good';
        if (intScore >= 60) return 'fair';
        return 'marginal';
    }

    /**
     * Calculate sales property score (0-100) - SALES SPECIFIC
     */
    calculateSalesPropertyScore(property) {
        let score = 50; // Base score
        
        // Undervaluation bonus (primary factor for sales)
        if (property.percentBelowMarket > 0) {
            score += Math.min(35, property.percentBelowMarket);
        }
        
        // Property type bonus (condos/co-ops typically more valuable)
        if (property.propertyType === 'condo') score += 5;
        else if (property.propertyType === 'co-op') score += 3;
        else if (property.propertyType === 'townhouse') score += 7;
        
        // HOA factor (lower HOA is better)
        if (!property.monthlyHoa || property.monthlyHoa === 0) {
            score += 5; // No HOA fees bonus
        } else if (property.monthlyHoa < 500) {
            score += 3; // Low HOA bonus
        } else if (property.monthlyHoa > 1500) {
            score -= 3; // High HOA penalty
        }
        
        // Confidence bonus
        score += Math.round((property.undervaluationConfidence || 0) * 0.05);
        
        // Amenity bonus
        score += Math.min(5, (property.amenities?.length || 0) * 0.5);
        
        return Math.max(0, Math.min(100, score));
    }

    /**
     * Calculate property grade based on score
     */
    calculatePropertyGrade(score) {
        if (score >= 98) return 'A+';
        if (score >= 93) return 'A';
        if (score >= 88) return 'B+';
        if (score >= 83) return 'B';
        if (score >= 75) return 'C+';
        if (score >= 65) return 'C';
        if (score >= 60) return 'D';
        return 'F';
    }

    /**
     * Get borough from neighborhood
     */
    getBoroughFromNeighborhood(neighborhood) {
        const manhattanNeighborhoods = [
            'soho', 'tribeca', 'west-village', 'east-village', 'lower-east-side',
            'financial-district', 'battery-park-city', 'chinatown', 'little-italy',
            'nolita', 'midtown', 'two-bridges', 'chelsea', 'gramercy-park', 'kips-bay',
            'murray-hill', 'midtown-east', 'midtown-west', 'hells-kitchen',
            'upper-east-side', 'upper-west-side', 'morningside-heights',
            'hamilton-heights', 'washington-heights', 'inwood', 'greenwich-village',
            'flatiron', 'noho', 'civic-center', 'hudson-square'
        ];
        
        const brooklynNeighborhoods = [
            'brooklyn-heights', 'dumbo', 'williamsburg', 'greenpoint', 'park-slope',
            'carroll-gardens', 'cobble-hill', 'boerum-hill', 'fort-greene',
            'prospect-heights', 'crown-heights', 'bedford-stuyvesant', 'bushwick', 
            'gowanus', 'clinton-hill', 'red-hook', 'prospect-lefferts-gardens', 
            'sunset-park', 'bay-ridge', 'bensonhurst', 'downtown-brooklyn', 
            'columbia-st-waterfront-district', 'vinegar-hill'
        ];
        
        const queensNeighborhoods = [
            'long-island-city', 'astoria', 'sunnyside', 'woodside', 'elmhurst',
            'jackson-heights', 'corona', 'flushing', 'forest-hills', 'ridgewood',
            'maspeth', 'rego-park'
        ];
        
        const bronxNeighborhoods = [
            'mott-haven', 'melrose', 'concourse', 'yankee-stadium', 'fordham',
            'belmont', 'tremont', 'mount-eden', 'south-bronx', 'highbridge',
            'university-heights', 'morrisania'
        ];
        
        const normalizedNeighborhood = neighborhood.toLowerCase();
        
        if (manhattanNeighborhoods.includes(normalizedNeighborhood)) return 'Manhattan';
        if (brooklynNeighborhoods.includes(normalizedNeighborhood)) return 'Brooklyn';
        if (queensNeighborhoods.includes(normalizedNeighborhood)) return 'Queens';
        if (bronxNeighborhoods.includes(normalizedNeighborhood)) return 'Bronx';
        
        return 'Unknown';
    }

    /**
     * Helper function for delays
     */
    delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    /**
     * Main comprehensive sales analysis entry point
     */
    async runComprehensiveSalesAnalysis(options = {}) {
        console.log('\n🚀 CLAUDE-POWERED SALES ANALYSIS STARTING...');
        console.log('=' .repeat(60));
        
        const startTime = Date.now();
        const results = {
            undervaluedSales: 0,
            totalAnalyzed: 0,
            neighborhoodsProcessed: 0,
            apiCallsUsed: 0,
            twoStageEfficiency: 0,
            errors: []
        };
        
        try {
            // Determine neighborhoods to process
            const neighborhoods = options.neighborhoods || 
                                (process.env.TEST_NEIGHBORHOOD ? [process.env.TEST_NEIGHBORHOOD] : 
                                 this.getHighPriorityNeighborhoods());
            
            console.log(`🎯 Processing ${neighborhoods.length} neighborhoods...`);
            
            // Process each neighborhood
            for (const neighborhood of neighborhoods) {
                try {
                    console.log(`\n📍 Analyzing ${neighborhood}...`);
                    
                    const neighborhoodResults = await this.analyzeNeighborhoodForSalesOpportunities(neighborhood);
                    
                    // Update totals
                    results.undervaluedSales += neighborhoodResults.undervaluedCount;
                    results.totalAnalyzed += neighborhoodResults.totalAnalyzed;
                    results.neighborhoodsProcessed++;
                    
                    console.log(`   ✅ ${neighborhood}: ${neighborhoodResults.undervaluedCount} undervalued sales found`);
                    
                } catch (error) {
                    console.error(`   ❌ Error in ${neighborhood}: ${error.message}`);
                    results.errors.push({ neighborhood, error: error.message });
                }
                
                // Rate limiting between neighborhoods
                await this.delay(1000);
            }
            
            // Final results
            const duration = (Date.now() - startTime) / 1000;
            results.apiCallsUsed = this.claudeAnalyzer.apiCallsUsed;
            
            console.log('\n🎉 CLAUDE SALES ANALYSIS COMPLETE!');
            console.log('=' .repeat(60));
            console.log(`⏱️ Duration: ${Math.round(duration)}s`);
            console.log(`📊 Total analyzed: ${results.totalAnalyzed} properties`);
            console.log(`🏠 Undervalued sales: ${results.undervaluedSales}`);
            console.log(`🤖 Claude API calls: ${results.apiCallsUsed}`);
            console.log(`💰 Estimated cost: ${(results.apiCallsUsed * 0.0006).toFixed(3)}`);
            
            return results;
            
        } catch (error) {
            console.error('💥 SALES ANALYSIS FAILED:', error.message);
            throw error;
        }
    }

    /**
     * Get analysis summary
     */
    async getAnalysisSummary() {
        try {
            // Get undervalued sales
            const { data: undervalued, error: undervaluedError } = await this.supabase
                .from('undervalued_sales')
                .select('*')
                .eq('status', 'active')
                .gte('discount_percent', this.undervaluationThreshold)
                .order('discount_percent', { ascending: false })
                .limit(10);

            return {
                topUndervaluedSales: undervalued || []
            };

        } catch (error) {
            console.error('Error getting summary:', error.message);
            return { topUndervaluedSales: [] };
        }
    }

    /**
     * Get usage statistics
     */
    getUsageStats() {
        return {
            apiCallsUsed: this.claudeAnalyzer.apiCallsUsed,
            estimatedCost: this.claudeAnalyzer.apiCallsUsed * 0.0006,
            twoStageEfficiency: this.twoStageCallsSaved
        };
    }

    /**
     * Run comprehensive sales analysis for multiple neighborhoods
     */
    async runComprehensiveAnalysis(neighborhoods, options = {}) {
        console.log('\n🏙️ Starting comprehensive sales analysis...');
        console.log(`   🎯 Target neighborhoods: ${neighborhoods.join(', ')}`);
        
        const overallResults = {
            totalNeighborhoods: neighborhoods.length,
            totalListings: 0,
            totalAnalyzed: 0,
            undervaluedCount: 0,
            savedCount: 0,
            neighborhoodResults: [],
            startTime: new Date(),
            errors: []
        };
        
        for (const neighborhood of neighborhoods) {
            try {
                const result = await this.analyzeNeighborhoodForSalesOpportunities(neighborhood, options);
                
                overallResults.totalListings += result.totalListings;
                overallResults.totalAnalyzed += result.totalAnalyzed;
                overallResults.undervaluedCount += result.undervaluedCount;
                overallResults.savedCount += result.savedCount;
                overallResults.neighborhoodResults.push(result);
                overallResults.errors.push(...result.errors);
                
            } catch (error) {
                console.error(`❌ Failed to analyze ${neighborhood}: ${error.message}`);
                overallResults.errors.push(`${neighborhood}: ${error.message}`);
            }
            
            // Delay between neighborhoods
            await this.delay(1000);
        }
        
        overallResults.endTime = new Date();
        overallResults.duration = Math.round((overallResults.endTime - overallResults.startTime) / 1000);
        
        this.printFinalResults(overallResults);
        return overallResults;
    }

    /**
     * Print final analysis results
     */
    printFinalResults(results) {
        console.log('\n' + '='.repeat(60));
        console.log(`⏱️ Duration: ${results.duration}s`);
        console.log(`🏘️ Neighborhoods: ${results.totalNeighborhoods}`);
        console.log(`📊 Total analyzed: ${results.totalAnalyzed} properties`);
        console.log(`🏠 Undervalued sales: ${results.undervaluedCount}`);
        console.log(`💾 Total saved: ${results.savedCount}`);
        console.log(`🤖 Claude API calls: ${this.claudeAnalyzer.apiCallsUsed}`);
        
        if (results.errors.length > 0) {
            console.log(`⚠️ Errors: ${results.errors.length}`);
        }
        
        console.log('='.repeat(60));
        console.log('\n🎉 CLAUDE SALES ANALYSIS COMPLETE!');
    }

    /**
     * Get high-priority neighborhoods for analysis
     */
    getHighPriorityNeighborhoods() {
        return [
            // Manhattan priority areas
            'east-village', 'west-village', 'lower-east-side', 'chinatown',
            'little-italy', 'nolita', 'soho', 'tribeca', 'financial-district',
            'two-bridges', 'chelsea', 'gramercy-park', 'kips-bay', 'murray-hill',
            
            // Brooklyn priority areas  
            'williamsburg', 'greenpoint', 'bushwick', 'bedford-stuyvesant',
            'crown-heights', 'prospect-heights', 'park-slope', 'gowanus',
            'carroll-gardens', 'cobble-hill', 'brooklyn-heights', 'dumbo',
            'fort-greene', 'clinton-hill', 'boerum-hill',
            
            // Queens priority areas
            'long-island-city', 'astoria', 'sunnyside', 'woodside',
            'jackson-heights', 'elmhurst', 'corona', 'ridgewood',
            
            // Bronx priority areas
            'mott-haven', 'melrose', 'concourse', 'south-bronx'
        ];
    }
}

module.exports = ClaudePoweredSalesSystem;
