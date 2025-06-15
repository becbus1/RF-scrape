// optimal-weekly-streeteasy.js
// Weekly refresh of ALL NYC neighborhoods, filter to undervalued properties only

require('dotenv').config();
const axios = require('axios');
const { createClient } = require('@supabase/supabase-js');
const { HIGH_PRIORITY_NEIGHBORHOODS, ALL_NYC_NEIGHBORHOODS } = require('./comprehensive-nyc-neighborhoods.js');

class OptimalWeeklyStreetEasy {
    constructor() {
        this.supabase = createClient(
            process.env.SUPABASE_URL,
            process.env.SUPABASE_ANON_KEY
        );
        
        this.rapidApiKey = process.env.RAPIDAPI_KEY;
        this.weeklyApiCalls = 0;
        
        // Market analysis for price per sqft thresholds
        this.marketThresholds = {
            // Rough price per sqft thresholds for "market rate" in each area
            'Manhattan': 1400,
            'Brooklyn': 900,
            'Queens': 700,
            'Bronx': 450,
            'Staten Island': 500,
            
            // Specific high-value neighborhoods
            'West Village': 1800,
            'SoHo': 1700,
            'Tribeca': 1600,
            'Park Slope': 1200,
            'Williamsburg': 1100,
            'DUMBO': 1300,
            'Long Island City': 1000,
            'Astoria': 800
        };
    }

    /**
     * Main weekly refresh function
     */
    async runWeeklyUndervaluedRefresh() {
        console.log('\n🗽 Starting Weekly Undervalued Property Refresh');
        console.log('='.repeat(60));
        console.log(`📋 Strategy: Fetch ALL neighborhoods → Filter 15%+ below market → Store only undervalued`);
        console.log(`🎯 Neighborhoods to check: ${HIGH_PRIORITY_NEIGHBORHOODS.length} high-priority areas\n`);

        const summary = {
            startTime: new Date(),
            neighborhoodsChecked: 0,
            totalPropertiesFetched: 0,
            undervaluedFound: 0,
            savedToDatabase: 0,
            apiCallsUsed: 0,
            errors: []
        };

        try {
            // Step 1: Clear old data
            await this.clearOldUndervaluedProperties();

            // Step 2: Process each high-priority neighborhood
            for (const neighborhood of HIGH_PRIORITY_NEIGHBORHOODS) {
                try {
                    console.log(`🔍 Processing ${neighborhood}...`);
                    
                    const properties = await this.fetchNeighborhoodProperties(neighborhood);
                    summary.totalPropertiesFetched += properties.length;
                    summary.apiCallsUsed++;
                    this.weeklyApiCalls++;

                    if (properties.length > 0) {
                        const undervalued = this.filterUndervaluedProperties(properties, neighborhood);
                        summary.undervaluedFound += undervalued.length;

                        if (undervalued.length > 0) {
                            const saved = await this.saveUndervaluedProperties(undervalued, neighborhood);
                            summary.savedToDatabase += saved;
                            console.log(`   ✅ ${neighborhood}: ${undervalued.length} undervalued found, ${saved} saved`);
                        } else {
                            console.log(`   📊 ${neighborhood}: ${properties.length} properties, none undervalued`);
                        }
                    } else {
                        console.log(`   ⚠️ ${neighborhood}: No properties returned`);
                    }

                    summary.neighborhoodsChecked++;

                    // Rate limiting
                    await this.delay(1000);

                } catch (error) {
                    console.error(`❌ Error processing ${neighborhood}:`, error.message);
                    summary.errors.push({ neighborhood, error: error.message });
                }

                // Safety check for API limits
                if (this.weeklyApiCalls >= 200) { // Safety limit
                    console.log('⚠️ Reached weekly API safety limit, stopping');
                    break;
                }
            }

            summary.endTime = new Date();
            summary.duration = (summary.endTime - summary.startTime) / 1000 / 60; // minutes

            this.logWeeklySummary(summary);
            await this.saveWeeklySummary(summary);

        } catch (error) {
            console.error('💥 Weekly refresh failed:', error.message);
            summary.errors.push({ error: error.message });
        }

        return summary;
    }

    /**
     * Fetch properties from a single neighborhood
     */
    async fetchNeighborhoodProperties(neighborhood) {
        try {
            const response = await axios.get(
                'https://streeteasy-api.p.rapidapi.com/properties/search',
                {
                    params: {
                        areas: neighborhood,
                        limit: 500, // Max per call
                        minPrice: 200000, // Reasonable minimum for NYC
                        maxPrice: 5000000, // Reasonable maximum
                        types: 'condo,coop,house' // All property types
                    },
                    headers: {
                        'X-RapidAPI-Key': this.rapidApiKey,
                        'X-RapidAPI-Host': 'streeteasy-api.p.rapidapi.com'
                    },
                    timeout: 15000
                }
            );

            if (response.data && Array.isArray(response.data)) {
                return response.data.map(property => ({
                    listing_id: property.id || `${property.address}-${property.price}`,
                    address: property.address,
                    neighborhood: neighborhood,
                    price: property.price,
                    sqft: property.sqft,
                    beds: property.beds,
                    baths: property.baths,
                    description: property.description || '',
                    url: property.url,
                    property_type: property.type,
                    fetched_date: new Date().toISOString()
                }));
            }

            return [];

        } catch (error) {
            console.error(`❌ API error for ${neighborhood}:`, error.message);
            throw error;
        }
    }

    /**
     * Filter properties that are 15%+ below market price per sqft
     */
    filterUndervaluedProperties(properties, neighborhood) {
        const undervalued = [];

        for (const property of properties) {
            if (!property.price || !property.sqft || property.sqft <= 0) {
                continue; // Skip properties without valid price/sqft data
            }

            const actualPricePerSqft = property.price / property.sqft;
            const marketThreshold = this.getMarketThreshold(neighborhood);
            const discountPercent = ((marketThreshold - actualPricePerSqft) / marketThreshold) * 100;

            // Property is undervalued if it's 15%+ below market rate per sqft
            if (discountPercent >= 15) {
                // Analyze description for distress signals
                const distressSignals = this.findDistressSignals(property.description);
                const warningSignals = this.findWarningSignals(property.description);
                
                // Calculate comprehensive score
                const score = this.calculateUndervaluationScore({
                    discountPercent,
                    distressSignals,
                    warningSignals,
                    neighborhood,
                    propertyType: property.property_type,
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

    /**
     * Get market threshold for neighborhood
     */
    getMarketThreshold(neighborhood) {
        // Check specific neighborhood first
        if (this.marketThresholds[neighborhood]) {
            return this.marketThresholds[neighborhood];
        }

        // Fall back to borough-level thresholds
        if (neighborhood.includes('Manhattan') || 
            ['West Village', 'East Village', 'SoHo', 'Tribeca', 'Chelsea', 'Upper East Side', 'Upper West Side'].some(n => neighborhood.includes(n))) {
            return this.marketThresholds['Manhattan'];
        }

        if (['Park Slope', 'Williamsburg', 'DUMBO', 'Brooklyn Heights', 'Fort Greene'].some(n => neighborhood.includes(n))) {
            return this.marketThresholds['Brooklyn'];
        }

        if (['Long Island City', 'Astoria', 'Sunnyside'].some(n => neighborhood.includes(n))) {
            return this.marketThresholds['Queens'];
        }

        if (neighborhood.includes('Bronx') || ['Mott Haven', 'South Bronx'].includes(neighborhood)) {
            return this.marketThresholds['Bronx'];
        }

        if (neighborhood.includes('Staten Island') || ['St. George', 'Stapleton'].includes(neighborhood)) {
            return this.marketThresholds['Staten Island'];
        }

        // Default fallback
        return 800;
    }

    /**
     * Find distress signals in description
     */
    findDistressSignals(description) {
        const distressKeywords = [
            'motivated seller', 'must sell', 'as-is', 'needs work', 'fixer-upper',
            'estate sale', 'inherited', 'price reduced', 'bring offers', 'cash only',
            'motivated', 'quick sale', 'investor special', 'tlc', 'handyman special'
        ];

        const text = description.toLowerCase();
        return distressKeywords.filter(keyword => text.includes(keyword));
    }

    /**
     * Find warning signals in description
     */
    findWarningSignals(description) {
        const warningKeywords = [
            'flood damage', 'water damage', 'foundation issues', 'structural',
            'fire damage', 'mold', 'asbestos', 'lead paint', 'no permits',
            'back taxes', 'liens', 'title issues', 'housing court'
        ];

        const text = description.toLowerCase();
        return warningKeywords.filter(keyword => text.includes(keyword));
    }

    /**
     * Calculate undervaluation score
     */
    calculateUndervaluationScore(factors) {
        let score = 0;

        // Discount percentage (0-50 points)
        score += Math.min(factors.discountPercent * 2, 50);

        // Distress signals bonus (0-20 points)
        score += Math.min(factors.distressSignals.length * 5, 20);

        // Neighborhood bonus (0-15 points)
        const premiumNeighborhoods = ['West Village', 'SoHo', 'Tribeca', 'Park Slope', 'Williamsburg'];
        if (premiumNeighborhoods.includes(factors.neighborhood)) {
            score += 15;
        } else if (factors.neighborhood.includes('Manhattan') || factors.neighborhood.includes('Brooklyn')) {
            score += 10;
        } else {
            score += 5;
        }

        // Size bonus (0-10 points)
        if (factors.sqft > 1000) score += 10;
        else if (factors.sqft > 700) score += 7;
        else score += 3;

        // Bedroom bonus (0-5 points)
        if (factors.beds >= 2) score += 5;

        // Warning penalty (0 to -15 points)
        score -= Math.min(factors.warningSignals.length * 5, 15);

        return Math.max(0, Math.round(score));
    }

    /**
     * Save undervalued properties to database
     */
    async saveUndervaluedProperties(properties, neighborhood) {
        try {
            const dbRecords = properties.map(property => ({
                listing_id: property.listing_id,
                address: property.address,
                neighborhood: property.neighborhood,
                price: property.price,
                sqft: property.sqft,
                beds: property.beds,
                baths: property.baths,
                description: property.description.substring(0, 500), // Truncate for storage
                url: property.url,
                property_type: property.property_type,
                actual_price_per_sqft: property.actual_price_per_sqft,
                market_price_per_sqft: property.market_price_per_sqft,
                discount_percent: property.discount_percent,
                potential_savings: property.potential_savings,
                distress_signals: property.distress_signals,
                warning_signals: property.warning_signals,
                undervaluation_score: property.undervaluation_score,
                analysis_date: property.analysis_date,
                status: 'active'
            }));

            const { data, error } = await this.supabase
                .from('undervalued_properties')
                .insert(dbRecords);

            if (error) {
                console.error(`❌ Database error for ${neighborhood}:`, error.message);
                return 0;
            }

            return dbRecords.length;

        } catch (error) {
            console.error(`❌ Save error for ${neighborhood}:`, error.message);
            return 0;
        }
    }

    /**
     * Clear old undervalued properties (weekly refresh)
     */
    async clearOldUndervaluedProperties() {
        try {
            const oneWeekAgo = new Date();
            oneWeekAgo.setDate(oneWeekAgo.getDate() - 7);

            const { data, error } = await this.supabase
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

    /**
     * Save weekly summary to database
     */
    async saveWeeklySummary(summary) {
        try {
            const { error } = await this.supabase
                .from('weekly_analysis_runs')
                .insert([{
                    run_date: summary.startTime,
                    neighborhoods_checked: summary.neighborhoodsChecked,
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

    /**
     * Log weekly summary
     */
    logWeeklySummary(summary) {
        console.log('\n📊 WEEKLY UNDERVALUED REFRESH COMPLETE');
        console.log('='.repeat(60));
        console.log(`⏱️ Duration: ${summary.duration.toFixed(1)} minutes`);
        console.log(`🗽 Neighborhoods checked: ${summary.neighborhoodsChecked}`);
        console.log(`📡 Total properties fetched: ${summary.totalPropertiesFetched}`);
        console.log(`🎯 Undervalued properties found: ${summary.undervaluedFound}`);
        console.log(`💾 Saved to database: ${summary.savedToDatabase}`);
        console.log(`📞 API calls used: ${summary.apiCallsUsed}`);
        
        if (summary.undervaluedFound > 0) {
            const avgDiscount = (summary.undervaluedFound / summary.totalPropertiesFetched * 100).toFixed(1);
            console.log(`📈 Undervaluation rate: ${avgDiscount}% of properties analyzed`);
        }

        if (summary.errors.length > 0) {
            console.log(`❌ Errors: ${summary.errors.length}`);
            summary.errors.slice(0, 5).forEach(err => {
                console.log(`   - ${err.neighborhood || 'General'}: ${err.error}`);
            });
        }

        console.log('\n🎉 Next week: Fresh data will be collected and old data refreshed!');
    }

    /**
     * Utility delay function
     */
    delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

// Main execution for weekly run
async function runWeeklyAnalysis() {
    if (!process.env.RAPIDAPI_KEY || !process.env.SUPABASE_URL || !process.env.SUPABASE_ANON_KEY) {
        console.error('❌ Missing required environment variables!');
        console.error('   RAPIDAPI_KEY, SUPABASE_URL, SUPABASE_ANON_KEY required');
        process.exit(1);
    }

    const analyzer = new OptimalWeeklyStreetEasy();
    
    try {
        console.log('🗽 Starting Optimal Weekly StreetEasy Analysis...\n');
        const results = await analyzer.runWeeklyUndervaluedRefresh();
        
        console.log('\n✅ Weekly analysis completed successfully!');
        console.log(`📊 Check your Supabase 'undervalued_properties' table for ${results.savedToDatabase} new deals!`);
        
        return results;
        
    } catch (error) {
        console.error('💥 Weekly analysis failed:', error.message);
        process.exit(1);
    }
}

// Export for use in scheduler
module.exports = OptimalWeeklyStreetEasy;

// Run if executed directly
if (require.main === module) {
    runWeeklyAnalysis().catch(console.error);
}
