// optimal-weekly-streeteasy.js
// FIXED VERSION: Correct API endpoints and parameters

require('dotenv').config();
const axios = require('axios');
const { createClient } = require('@supabase/supabase-js');

const VALID_STREETEASY_SLUGS = new Set([
    "west-village", "east-village", "soho", "tribeca", "chelsea",
    "upper-east-side", "upper-west-side", "financial-district", "lower-east-side",
    "gramercy-park", "murray-hill", "hells-kitchen", "midtown",
    "park-slope", "williamsburg", "dumbo", "brooklyn-heights", "fort-greene",
    "prospect-heights", "crown-heights", "bedford-stuyvesant", "greenpoint",
    "red-hook", "carroll-gardens", "bushwick", "sunset-park",
    "long-island-city", "hunters-point", "astoria", "sunnyside",
    "woodside", "jackson-heights", "forest-hills", "kew-gardens",
    "mott-haven", "concourse", "fordham", "riverdale",
    "saint-george", "stapleton", "new-brighton"
]);

const { HIGH_PRIORITY_NEIGHBORHOODS } = require('./comprehensive-nyc-neighborhoods.js');

class OptimalWeeklyStreetEasy {
    constructor() {
        this.supabase = createClient(
            process.env.SUPABASE_URL,
            process.env.SUPABASE_ANON_KEY
        );
        
        this.rapidApiKey = process.env.RAPIDAPI_KEY;
        this.apiCallsUsed = 0;
        
        // AGGRESSIVE rate limiting settings
        this.baseDelay = 15000; // 15 seconds between calls
        this.maxRetries = 3;
        this.retryBackoffMultiplier = 2; // 15s, 30s, 60s on retries
        
        // Market thresholds for analysis
        this.marketThresholds = {
            'west-village': 1800, 'soho': 1700, 'tribeca': 1600,
            'park-slope': 1200, 'williamsburg': 1100, 'dumbo': 1300,
            'long-island-city': 1000, 'astoria': 800, 'mott-haven': 450,
            'saint-george': 500, 'default': 800
        };
    }

    /**
     * Main weekly refresh with aggressive rate limiting and duplicate prevention
     */
    async runWeeklyUndervaluedRefresh() {
        console.log('\n🗽 Starting FIXED Weekly StreetEasy Analysis');
        console.log('⏱️ Using correct API endpoints with 15+ second delays');
        console.log('🔧 Fixed: /sales/search endpoint with areas parameter');
        console.log('🔒 Duplicate prevention enabled');
        console.log('='.repeat(60));

        const summary = {
            startTime: new Date(),
            neighborhoodsProcessed: 0,
            totalPropertiesFetched: 0,
            undervaluedFound: 0,
            savedToDatabase: 0,
            updatedInDatabase: 0,
            duplicatesSkipped: 0,
            apiCallsUsed: 0,
            errors: [],
            rateLimitHits: 0
        };

        try {
            // Clear old data
            await this.clearOldUndervaluedProperties();

            // Get valid neighborhoods only
            const validNeighborhoods = this.getValidNeighborhoods();
            console.log(`🎯 Processing ${validNeighborhoods.length} valid neighborhoods with 15s+ delays\n`);

            // Process each neighborhood with aggressive spacing
            for (let i = 0; i < validNeighborhoods.length; i++) {
                const neighborhood = validNeighborhoods[i];
                
                try {
                    console.log(`🔍 [${i + 1}/${validNeighborhoods.length}] Processing ${neighborhood}...`);
                    
                    // Apply base delay before each call (except first)
                    if (i > 0) {
                        const delay = this.calculateDelay(i);
                        console.log(`   ⏰ Waiting ${delay/1000}s to avoid rate limits...`);
                        await this.delay(delay);
                    }
                    
                    const properties = await this.fetchNeighborhoodPropertiesWithRetry(neighborhood);
                    summary.totalPropertiesFetched += properties.length;
                    summary.apiCallsUsed++;

                    if (properties.length > 0) {
                        const undervalued = this.filterUndervaluedProperties(properties, neighborhood);
                        summary.undervaluedFound += undervalued.length;

                        if (undervalued.length > 0) {
                            const saveResult = await this.saveUndervaluedPropertiesWithStats(undervalued, neighborhood);
                            summary.savedToDatabase += saveResult.newCount;
                            summary.updatedInDatabase += saveResult.updateCount;
                            summary.duplicatesSkipped += saveResult.duplicateCount;
                            console.log(`   ✅ ${neighborhood}: ${saveResult.newCount} new, ${saveResult.updateCount} updated, ${saveResult.duplicateCount} duplicates`);
                        } else {
                            console.log(`   📊 ${neighborhood}: ${properties.length} properties, none undervalued`);
                        }
                    } else {
                        console.log(`   📊 ${neighborhood}: No properties returned`);
                    }

                    summary.neighborhoodsProcessed++;

                } catch (error) {
                    const isRateLimit = error.response?.status === 429;
                    const is404 = error.response?.status === 404;
                    
                    if (isRateLimit) {
                        summary.rateLimitHits++;
                        console.error(`   ❌ RATE LIMITED on ${neighborhood} - increasing delays`);
                        
                        // Exponentially increase base delay after rate limit hits
                        this.baseDelay = Math.min(this.baseDelay * 1.5, 60000); // Max 60s
                        console.log(`   ⏰ New base delay: ${this.baseDelay/1000}s`);
                        
                        // Wait extra long after rate limit
                        await this.delay(this.baseDelay * 2);
                    } else if (is404) {
                        console.error(`   ❌ 404 ERROR on ${neighborhood} - endpoint/parameter issue`);
                    } else {
                        console.error(`   ❌ Error with ${neighborhood}: ${error.message}`);
                    }
                    
                    summary.errors.push({ 
                        neighborhood, 
                        error: error.message,
                        isRateLimit,
                        is404 
                    });
                }

                // Log progress every 5 neighborhoods
                if ((i + 1) % 5 === 0) {
                    const elapsed = (Date.now() - summary.startTime) / 1000 / 60;
                    console.log(`\n📊 Progress: ${i + 1}/${validNeighborhoods.length} neighborhoods (${elapsed.toFixed(1)}min elapsed)`);
                    console.log(`📊 Stats: ${summary.undervaluedFound} undervalued, ${summary.rateLimitHits} rate limits, ${summary.errors.filter(e => e.is404).length} 404s\n`);
                }
            }

            summary.endTime = new Date();
            summary.duration = (summary.endTime - summary.startTime) / 1000 / 60;

            this.logWeeklySummary(summary);
            await this.saveWeeklySummary(summary);

        } catch (error) {
            console.error('💥 Weekly refresh failed:', error.message);
            summary.errors.push({ error: error.message });
        }

        return summary;
    }

    /**
     * Calculate progressive delay - gets longer as we make more calls
     */
    calculateDelay(callIndex) {
        // Start with base delay, increase gradually
        const progressiveIncrease = Math.floor(callIndex / 5) * 2000; // +2s every 5 calls
        return this.baseDelay + progressiveIncrease;
    }

    /**
     * Fetch with retry logic and exponential backoff
     */
    async fetchNeighborhoodPropertiesWithRetry(neighborhood) {
        let lastError;
        
        for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
            try {
                return await this.fetchNeighborhoodProperties(neighborhood);
                
            } catch (error) {
                lastError = error;
                const isRateLimit = error.response?.status === 429;
                const is404 = error.response?.status === 404;
                
                if (is404) {
                    // Don't retry 404s - it's an endpoint/parameter issue
                    throw new Error(`404 Not Found - Check API endpoint and parameters for ${neighborhood}`);
                } else if (isRateLimit && attempt < this.maxRetries) {
                    const backoffDelay = this.baseDelay * Math.pow(this.retryBackoffMultiplier, attempt);
                    console.log(`   🔄 Rate limited (attempt ${attempt}), waiting ${backoffDelay/1000}s before retry...`);
                    await this.delay(backoffDelay);
                } else if (attempt < this.maxRetries) {
                    // For non-rate-limit errors, shorter delay
                    console.log(`   🔄 Error (attempt ${attempt}), waiting 5s before retry...`);
                    await this.delay(5000);
                } else {
                    // Final attempt failed
                    throw lastError;
                }
            }
        }
        
        throw lastError;
    }

    /**
     * FIXED: Fetch properties from StreetEasy API with correct endpoint and parameters
     */
    async fetchNeighborhoodProperties(neighborhood) {
        // FIXED: Use correct endpoint and parameter names
        const response = await axios.get(
            'https://streeteasy-api.p.rapidapi.com/sales/search', // ✅ Correct endpoint
            {
                params: {
                    areas: neighborhood,        // ✅ Correct parameter name (not 'location')
                    limit: 200,                // Reasonable limit to avoid timeouts
                    minPrice: 200000,
                    maxPrice: 5000000
                },
                headers: {
                    'X-RapidAPI-Key': this.rapidApiKey,
                    'X-RapidAPI-Host': 'streeteasy-api.p.rapidapi.com'
                },
                timeout: 30000 // 30s timeout
            }
        );

        // Handle different response formats
        let propertiesData = [];
        
        if (response.data) {
            // Check if response has listings array
            if (response.data.listings && Array.isArray(response.data.listings)) {
                propertiesData = response.data.listings;
            }
            // Check if response is directly an array
            else if (Array.isArray(response.data)) {
                propertiesData = response.data;
            }
            // Check for other possible property arrays
            else if (response.data.properties && Array.isArray(response.data.properties)) {
                propertiesData = response.data.properties;
            }
            // Check for results array
            else if (response.data.results && Array.isArray(response.data.results)) {
                propertiesData = response.data.results;
            }
            else {
                console.warn(`   ⚠️ Unexpected response format for ${neighborhood}:`, Object.keys(response.data));
                return [];
            }
        }

        // Map to consistent format
        return propertiesData.map(property => ({
            listing_id: property.id || property.listing_id || `${property.address}-${property.price}`,
            address: property.address || property.street_address || 'Address not available',
            neighborhood: neighborhood,
            price: property.price || property.list_price || 0,
            sqft: property.sqft || property.square_feet || property.size || 0,
            beds: property.beds || property.bedrooms || 0,
            baths: property.baths || property.bathrooms || 0,
            description: property.description || property.details || '',
            url: property.url || property.link || property.streeteasy_url || '',
            property_type: property.type || property.property_type || 'unknown',
            days_on_market: property.days_on_market || property.dom || 0,
            fetched_date: new Date().toISOString()
        }));
    }

    /**
     * Get valid neighborhoods that exist in StreetEasy API
     */
    getValidNeighborhoods() {
        return HIGH_PRIORITY_NEIGHBORHOODS
            .map(n => n.toLowerCase().replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, ''))
            .filter(slug => VALID_STREETEASY_SLUGS.has(slug))
            .slice(0, 20); // Limit to top 20 to keep runtime reasonable
    }

    /**
     * Filter undervalued properties
     */
    filterUndervaluedProperties(properties, neighborhood) {
        const undervalued = [];

        for (const property of properties) {
            if (!property.price || !property.sqft || property.sqft <= 0) {
                continue;
            }

            const actualPricePerSqft = property.price / property.sqft;
            const marketThreshold = this.marketThresholds[neighborhood] || this.marketThresholds.default;
            const discountPercent = ((marketThreshold - actualPricePerSqft) / marketThreshold) * 100;

            if (discountPercent >= 15) {
                const distressSignals = this.findDistressSignals(property.description);
                const warningSignals = this.findWarningSignals(property.description);
                const score = this.calculateUndervaluationScore({
                    discountPercent,
                    distressSignals,
                    warningSignals,
                    neighborhood,
                    sqft: property.sqft,
                    beds: property.beds
                });

                undervalued.push({
                    ...property,
                    actual_price_per_sqft: Math.round(actualPricePerSqft),
                    market_price_per_sqft: marketThreshold,
                    discount_percent: Math.round(discountPercent * 10) / 10,
                    potential_savings: Math.round((marketThreshold - actualPricePerSqft) * property.sqft),
                    distress_signals: distressSignals,
                    warning_signals: warningSignals,
                    undervaluation_score: score,
                    analysis_date: new Date().toISOString()
                });
            }
        }

        return undervalued;
    }

    findDistressSignals(description) {
        const distressKeywords = [
            'motivated seller', 'must sell', 'as-is', 'needs work', 'fixer-upper',
            'estate sale', 'inherited', 'price reduced', 'bring offers', 'cash only'
        ];
        const text = description.toLowerCase();
        return distressKeywords.filter(keyword => text.includes(keyword));
    }

    findWarningSignals(description) {
        const warningKeywords = [
            'flood damage', 'water damage', 'foundation issues', 'structural',
            'fire damage', 'mold', 'asbestos', 'lead paint', 'no permits'
        ];
        const text = description.toLowerCase();
        return warningKeywords.filter(keyword => text.includes(keyword));
    }

    calculateUndervaluationScore(factors) {
        let score = Math.min(factors.discountPercent * 2, 50);
        score += Math.min(factors.distressSignals.length * 5, 20);
        if (factors.sqft > 1000) score += 10;
        else if (factors.sqft > 700) score += 7;
        else score += 3;
        if (factors.beds >= 2) score += 5;
        score -= Math.min(factors.warningSignals.length * 5, 15);
        return Math.max(0, Math.round(score));
    }

    /**
     * Save to database with duplicate prevention and return detailed stats
     */
    async saveUndervaluedPropertiesWithStats(properties, neighborhood) {
        if (!properties || properties.length === 0) {
            return { newCount: 0, updateCount: 0, duplicateCount: 0 };
        }

        console.log(`   💾 Checking ${properties.length} properties for duplicates...`);
        
        let newCount = 0;
        let duplicateCount = 0;
        let updateCount = 0;

        try {
            for (const property of properties) {
                // Check for existing property by listing_id or address+price combination
                const { data: existing, error: checkError } = await this.supabase
                    .from('undervalued_properties')
                    .select('id, undervaluation_score, analysis_date')
                    .or(`listing_id.eq.${property.listing_id},and(address.eq."${property.address}",price.eq.${property.price})`)
                    .single();

                if (checkError && checkError.code !== 'PGRST116') {
                    // PGRST116 = no rows found (not an error)
                    console.error(`   ❌ Error checking for existing property:`, checkError.message);
                    continue;
                }

                const dbRecord = {
                    listing_id: property.listing_id,
                    address: property.address,
                    neighborhood: property.neighborhood,
                    price: property.price,
                    sqft: property.sqft,
                    beds: property.beds,
                    baths: property.baths,
                    description: (property.description || '').substring(0, 500),
                    url: property.url,
                    property_type: property.property_type,
                    actual_price_per_sqft: property.actual_price_per_sqft,
                    market_price_per_sqft: property.market_price_per_sqft,
                    discount_percent: property.discount_percent,
                    potential_savings: property.potential_savings,
                    distress_signals: property.distress_signals || [],
                    warning_signals: property.warning_signals || [],
                    undervaluation_score: property.undervaluation_score,
                    analysis_date: property.analysis_date,
                    status: 'active'
                };

                if (existing) {
                    // Property exists - decide whether to update
                    const existingScore = existing.undervaluation_score || 0;
                    const newScore = property.undervaluation_score || 0;
                    
                    // Update if score improved significantly (5+ points) or data is much newer
                    const scoreImproved = newScore > (existingScore + 5);
                    const isNewerData = new Date(property.analysis_date) > new Date(existing.analysis_date);
                    
                    if (scoreImproved || (isNewerData && newScore >= existingScore)) {
                        // Update existing record
                        const { error: updateError } = await this.supabase
                            .from('undervalued_properties')
                            .update(dbRecord)
                            .eq('id', existing.id);

                        if (updateError) {
                            console.error(`   ❌ Error updating ${property.address}:`, updateError.message);
                        } else {
                            console.log(`   🔄 Updated: ${property.address} (score: ${existingScore} → ${newScore})`);
                            updateCount++;
                        }
                    } else {
                        console.log(`   ⏭️ Skipped duplicate: ${property.address} (score: ${newScore}, existing: ${existingScore})`);
                        duplicateCount++;
                    }
                } else {
                    // New property - insert it
                    const { error: insertError } = await this.supabase
                        .from('undervalued_properties')
                        .insert([dbRecord]);

                    if (insertError) {
                        console.error(`   ❌ Error inserting ${property.address}:`, insertError.message);
                    } else {
                        console.log(`   ✅ Added: ${property.address} (${property.discount_percent}% below market, score: ${property.undervaluation_score})`);
                        newCount++;
                    }
                }

                // Small delay to avoid overwhelming database
                await this.delay(100);
            }

            return { newCount, updateCount, duplicateCount };

        } catch (error) {
            console.error(`❌ Save error for ${neighborhood}:`, error.message);
            return { newCount: 0, updateCount: 0, duplicateCount: 0 };
        }
    }

    async clearOldUndervaluedProperties() {
        try {
            const oneWeekAgo = new Date();
            oneWeekAgo.setDate(oneWeekAgo.getDate() - 7);

            const { error } = await this.supabase
                .from('undervalued_properties')
                .delete()
                .lt('analysis_date', oneWeekAgo.toISOString());

            if (error) {
                console.error('❌ Error clearing old properties:', error.message);
            } else {
                console.log(`🧹 Cleared old properties from database`);
            }
        } catch (error) {
            console.error('❌ Clear old properties error:', error.message);
        }
    }

    async saveWeeklySummary(summary) {
        try {
            const { error } = await this.supabase
                .from('weekly_analysis_runs')
                .insert([{
                    run_date: summary.startTime,
                    neighborhoods_checked: summary.neighborhoodsProcessed,
                    total_properties_fetched: summary.totalPropertiesFetched,
                    undervalued_found: summary.undervaluedFound,
                    saved_to_database: summary.savedToDatabase,
                    api_calls_used: summary.apiCallsUsed,
                    duration_minutes: Math.round(summary.duration),
                    errors: summary.errors,
                    completed: true
                }]);

            if (error) {
                console.error('❌ Error saving weekly summary:', error.message);
            } else {
                console.log('✅ Weekly summary saved to database');
            }
        } catch (error) {
            console.error('❌ Save summary error:', error.message);
        }
    }

    logWeeklySummary(summary) {
        console.log('\n📊 FIXED WEEKLY ANALYSIS COMPLETE');
        console.log('='.repeat(60));
        console.log(`⏱️ Duration: ${summary.duration.toFixed(1)} minutes`);
        console.log(`🗽 Neighborhoods processed: ${summary.neighborhoodsProcessed}`);
        console.log(`📡 Total properties fetched: ${summary.totalPropertiesFetched}`);
        console.log(`🎯 Undervalued properties found: ${summary.undervaluedFound}`);
        console.log(`💾 New properties saved: ${summary.savedToDatabase}`);
        console.log(`🔄 Properties updated: ${summary.updatedInDatabase}`);
        console.log(`⏭️ Duplicates skipped: ${summary.duplicatesSkipped}`);
        console.log(`📞 API calls used: ${summary.apiCallsUsed}`);
        console.log(`⚡ Rate limit hits: ${summary.rateLimitHits}`);
        console.log(`⏰ Final delay setting: ${this.baseDelay/1000}s between calls`);
        
        if (summary.errors.length > 0) {
            const rateLimitErrors = summary.errors.filter(e => e.isRateLimit).length;
            const notFoundErrors = summary.errors.filter(e => e.is404).length;
            const otherErrors = summary.errors.length - rateLimitErrors - notFoundErrors;
            console.log(`❌ Errors: ${summary.errors.length} total (${rateLimitErrors} rate limits, ${notFoundErrors} 404s, ${otherErrors} other)`);
            
            if (notFoundErrors > 0) {
                console.log(`🔧 404 Errors suggest API endpoint/parameter issues - check StreetEasy API docs`);
            }
        }

        if (summary.savedToDatabase > 0) {
            console.log('\n🎉 SUCCESS: Found and saved undervalued properties!');
        } else {
            console.log('\n📊 No undervalued properties found (normal in competitive NYC market)');
        }
        
        const successRate = summary.neighborhoodsProcessed > 0 ? 
            ((summary.neighborhoodsProcessed - summary.errors.filter(e => e.is404).length) / summary.apiCallsUsed * 100).toFixed(1) : '0';
        console.log(`📈 Success rate: ${successRate}% of API calls succeeded (excluding 404s)`);
        
        const totalDbOperations = summary.savedToDatabase + summary.updatedInDatabase + summary.duplicatesSkipped;
        if (totalDbOperations > 0) {
            console.log(`🔒 Duplicate prevention: ${summary.duplicatesSkipped}/${totalDbOperations} (${(summary.duplicatesSkipped/totalDbOperations*100).toFixed(1)}%) duplicates prevented`);
        }
    }

    delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

// Main execution
async function runWeeklyAnalysis() {
    if (!process.env.RAPIDAPI_KEY || !process.env.SUPABASE_URL || !process.env.SUPABASE_ANON_KEY) {
        console.error('❌ Missing required environment variables!');
        console.error('   RAPIDAPI_KEY, SUPABASE_URL, SUPABASE_ANON_KEY required');
        process.exit(1);
    }

    const analyzer = new OptimalWeeklyStreetEasy();
    
    try {
        console.log('🗽 Starting FIXED Weekly StreetEasy Analysis...\n');
        console.log('🔧 Changes made:');
        console.log('   - Fixed endpoint: /sales/search (was /sales/active)');
        console.log('   - Fixed parameter: areas (was location)');
        console.log('   - Added 404 error handling');
        console.log('   - Improved response format handling\n');
        
        const results = await analyzer.runWeeklyUndervaluedRefresh();
        
        console.log('\n✅ Fixed analysis completed!');
        
        if (results.errors.filter(e => e.is404).length > 0) {
            console.log('\n⚠️ If you still get 404 errors, double-check:');
            console.log('   1. StreetEasy API documentation for exact endpoints');
            console.log('   2. Your RapidAPI subscription is active');
            console.log('   3. Neighborhood names match API requirements');
        }
        
        console.log(`📊 Check your Supabase 'undervalued_properties' table for ${results.savedToDatabase} new deals!`);
        
        return results;
        
    } catch (error) {
        console.error('💥 Fixed analysis failed:', error.message);
        process.exit(1);
    }
}

// Export for use in scheduler
module.exports = OptimalWeeklyStreetEasy;

// Run if executed directly
if (require.main === module) {
    runWeeklyAnalysis().catch(console.error);
}
