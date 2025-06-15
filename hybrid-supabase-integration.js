// hybrid-supabase-integration.js
// Weekly public data analysis with targeted description enhancement

const { createClient } = require('@supabase/supabase-js');
const HybridRedfinAnalyzer = require('./hybrid-redfin-analyzer.js');

class HybridPropertyTracker {
    constructor(supabaseUrl, supabaseKey) {
        this.supabase = createClient(supabaseUrl, supabaseKey);
        this.analyzer = new HybridRedfinAnalyzer();
    }

    /**
     * Run weekly hybrid analysis and store results
     */
    async runWeeklyAnalysis(criteria = {}) {
        console.log(`🗽 Starting weekly NYC hybrid analysis at ${new Date().toISOString()}`);

        const defaultCriteria = {
            minDiscountPercent: 15,  // At least 15% below market
            maxDaysOnMarket: 90,     // Listed within 90 days
            minPrice: 300000,        // $300K minimum for NYC
            maxPrice: 2500000,       // $2.5M maximum
            ...criteria
        };

        const summary = {
            startTime: new Date().toISOString(),
            analysisType: 'hybrid_weekly',
            criteria: defaultCriteria,
            publicDataDate: null,
            totalPropertiesAnalyzed: 0,
            undervaluedFound: 0,
            newListingsAdded: 0,
            descriptionsEnhanced: 0,
            errors: []
        };

        try {
            // Run complete hybrid analysis
            console.log('📊 Running hybrid analysis...');
            const results = await this.analyzer.runCompleteAnalysis(defaultCriteria);
            
            summary.totalPropertiesAnalyzed = results.summary.totalAnalyzed;
            summary.undervaluedFound = results.undervaluedProperties.length;
            summary.descriptionsEnhanced = results.summary.withDescriptions;

            if (results.undervaluedProperties.length > 0) {
                // Save new properties to database
                const newListings = await this.saveUndervaluedProperties(results.undervaluedProperties);
                summary.newListingsAdded = newListings;

                // Save market analysis data
                await this.saveMarketAnalysis(results.marketAverages);
            }

            summary.endTime = new Date().toISOString();
            summary.duration = new Date(summary.endTime) - new Date(summary.startTime);

            // Save analysis summary
            await this.saveAnalysisSummary(summary);

            console.log('\n🗽 WEEKLY HYBRID ANALYSIS COMPLETE');
            console.log('='.repeat(50));
            console.log(`📊 Properties analyzed: ${summary.totalPropertiesAnalyzed}`);
            console.log(`🎯 Undervalued properties found: ${summary.undervaluedFound}`);
            console.log(`💾 New listings added: ${summary.newListingsAdded}`);
            console.log(`📝 Descriptions enhanced: ${summary.descriptionsEnhanced}`);
            console.log(`⏱️ Duration: ${Math.round(summary.duration / 1000 / 60)} minutes`);

            return { summary, results };

        } catch (error) {
            console.error(`❌ Weekly analysis failed:`, error.message);
            summary.errors.push({ error: error.message, timestamp: new Date().toISOString() });
            summary.endTime = new Date().toISOString();
            await this.saveAnalysisSummary(summary);
            throw error;
        }
    }

    /**
     * Save undervalued properties to database with enhanced data
     */
    async saveUndervaluedProperties(properties) {
        console.log(`💾 Saving ${properties.length} undervalued properties to database...`);

        let newCount = 0;
        let updateCount = 0;

        for (const property of properties) {
            try {
                // Check if property already exists (by location + price)
                const { data: existing } = await this.supabase
                    .from('listings')
                    .select('id, score')
                    .eq('address', property.location)
                    .eq('price', `$${property.listingPrice.toLocaleString()}`)
                    .single();

                // Prepare enhanced data for database
                const listingData = {
                    address: property.location,
                    price: `$${property.listingPrice.toLocaleString()}`,
                    beds: null, // Not available in public data
                    sqft: null, // Not available in public data  
                    zip: property.zip,
                    link: null, // Would be populated if we had property URLs
                    score: property.finalScore,
                    percent_below_market: property.percentBelowMarket,
                    warning_tags: property.warningTags || [],
                    // Enhanced fields
                    expected_price: property.expectedPrice,
                    days_on_market: property.daysOnMarket,
                    distress_signals: property.distressSignals || [],
                    comparison_level: property.comparisonLevel,
                    reasoning: property.reasoning,
                    has_description: !!property.description,
                    description_snippet: property.description ? property.description.substring(0, 200) : null,
                    analysis_date: new Date().toISOString()
                };

                if (existing) {
                    // Update existing property if score improved
                    if (property.finalScore > existing.score) {
                        const { error } = await this.supabase
                            .from('listings')
                            .update(listingData)
                            .eq('id', existing.id);

                        if (error) {
                            console.error(`❌ Error updating ${property.location}:`, error.message);
                        } else {
                            console.log(`🔄 Updated: ${property.location} (Score: ${existing.score} → ${property.finalScore})`);
                            updateCount++;
                        }
                    } else {
                        console.log(`   ⏭️ Skipping existing: ${property.location} (Score: ${property.finalScore})`);
                    }
                } else {
                    // Insert new property
                    const { error } = await this.supabase
                        .from('listings')
                        .insert([listingData]);

                    if (error) {
                        console.error(`❌ Error saving ${property.location}:`, error.message);
                    } else {
                        console.log(`✅ Added: ${property.location} (${property.percentBelowMarket.toFixed(1)}% below market, Score: ${property.finalScore})`);
                        newCount++;
                    }
                }

            } catch (error) {
                console.error(`❌ Error processing ${property.location}:`, error.message);
            }
        }

        console.log(`💾 Database update complete: ${newCount} new, ${updateCount} updated`);
        return newCount;
    }

    /**
     * Save market analysis data for trend tracking
     */
    async saveMarketAnalysis(marketAverages) {
        try {
            console.log('📈 Saving market analysis data...');

            const marketData = {
                analysis_date: new Date().toISOString(),
                analysis_type: 'weekly_public_data',
                city_averages: marketAverages.cityLevel,
                zip_averages: marketAverages.zipLevel,
                neighborhood_averages: marketAverages.neighborhoodLevel,
                total_cities: Object.keys(marketAverages.cityLevel).length,
                total_zips: Object.keys(marketAverages.zipLevel).length,
                total_neighborhoods: Object.keys(marketAverages.neighborhoodLevel).length
            };

            const { error } = await this.supabase
                .from('market_analysis')
                .insert([marketData]);

            if (error) {
                console.error('❌ Error saving market analysis:', error.message);
            } else {
                console.log('✅ Market analysis data saved');
            }

        } catch (error) {
            console.error('❌ Error saving market analysis:', error.message);
        }
    }

    /**
     * Save analysis summary for tracking performance
     */
    async saveAnalysisSummary(summary) {
        try {
            const { error } = await this.supabase
                .from('analysis_runs')
                .insert([{
                    run_date: summary.startTime,
                    analysis_type: summary.analysisType,
                    criteria: summary.criteria,
                    total_properties_analyzed: summary.totalPropertiesAnalyzed,
                    undervalued_found: summary.undervaluedFound,
                    new_listings_added: summary.newListingsAdded,
                    descriptions_enhanced: summary.descriptionsEnhanced,
                    duration_minutes: summary.duration ? Math.round(summary.duration / 1000 / 60) : null,
                    errors: summary.errors,
                    completed: !summary.errors.length
                }]);

            if (error) {
                console.error('❌ Error saving analysis summary:', error.message);
            } else {
                console.log('✅ Analysis summary saved to database');
            }
        } catch (error) {
            console.error('❌ Error saving analysis summary:', error.message);
        }
    }

    /**
     * Get latest undervalued properties with enhanced data
     */
    async getLatestProperties(limit = 50, minScore = 40) {
        try {
            const { data, error } = await this.supabase
                .from('listings')
                .select('*')
                .gte('score', minScore)
                .order('analysis_date', { ascending: false })
                .limit(limit);

            if (error) {
                throw error;
            }

            return data;
        } catch (error) {
            console.error('❌ Error fetching latest properties:', error.message);
            return [];
        }
    }

    /**
     * Get properties with distress signals
     */
    async getPropertiesWithDistressSignals(limit = 20) {
        try {
            const { data, error } = await this.supabase
                .from('listings')
                .select('*')
                .not('distress_signals', 'eq', '{}')
                .order('score', { ascending: false })
                .limit(limit);

            if (error) {
                throw error;
            }

            console.log(`🚨 Found ${data.length} properties with distress signals`);
            return data;
        } catch (error) {
            console.error('❌ Error fetching properties with distress signals:', error.message);
            return [];
        }
    }

    /**
     * Get market trend analysis
     */
    async getMarketTrends(days = 30) {
        try {
            const cutoffDate = new Date();
            cutoffDate.setDate(cutoffDate.getDate() - days);

            const { data, error } = await this.supabase
                .from('market_analysis')
                .select('*')
                .gte('analysis_date', cutoffDate.toISOString())
                .order('analysis_date', { ascending: false });

            if (error) {
                throw error;
            }

            return data;
        } catch (error) {
            console.error('❌ Error fetching market trends:', error.message);
            return [];
        }
    }

    /**
     * Get analysis performance metrics
     */
    async getAnalysisMetrics() {
        try {
            const { data: runs, error: runsError } = await this.supabase
                .from('analysis_runs')
                .select('*')
                .order('run_date', { ascending: false })
                .limit(10);

            if (runsError) {
                throw runsError;
            }

            const { data: listings, error: listingsError } = await this.supabase
                .from('listings')
                .select('score, percent_below_market, has_description, analysis_date');

            if (listingsError) {
                throw listingsError;
            }

            const metrics = {
                recentRuns: runs,
                totalProperties: listings.length,
                avgScore: listings.reduce((sum, l) => sum + l.score, 0) / listings.length,
                avgPercentBelow: listings.reduce((sum, l) => sum + l.percent_below_market, 0) / listings.length,
                withDescriptions: listings.filter(l => l.has_description).length,
                scoreDistribution: {
                    excellent: listings.filter(l => l.score >= 80).length,
                    good: listings.filter(l => l.score >= 60 && l.score < 80).length,
                    fair: listings.filter(l => l.score >= 40 && l.score < 60).length,
                    poor: listings.filter(l => l.score < 40).length
                }
            };

            return metrics;
        } catch (error) {
            console.error('❌ Error getting analysis metrics:', error.message);
            return null;
        }
    }

    /**
     * Setup enhanced database schema
     */
    async setupEnhancedDatabase() {
        console.log('🔧 Setting up enhanced database schema...');

        try {
            // Enhanced listings table
            const enhancedListingsSchema = `
                -- Enhanced listings table with hybrid analysis fields
                CREATE TABLE IF NOT EXISTS listings (
                    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
                    address text,
                    price text,
                    beds text,
                    sqft text,
                    zip text,
                    link text,
                    score int,
                    percent_below_market real,
                    warning_tags text[],
                    created_at timestamp DEFAULT now(),
                    
                    -- Enhanced hybrid analysis fields
                    expected_price real,
                    days_on_market int,
                    distress_signals text[],
                    comparison_level text,
                    reasoning text,
                    has_description boolean DEFAULT false,
                    description_snippet text,
                    analysis_date timestamp DEFAULT now()
                );
            `;

            // Market analysis table
            const marketAnalysisSchema = `
                CREATE TABLE IF NOT EXISTS market_analysis (
                    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
                    analysis_date timestamp DEFAULT now(),
                    analysis_type text,
                    city_averages jsonb,
                    zip_averages jsonb,
                    neighborhood_averages jsonb,
                    total_cities int,
                    total_zips int,
                    total_neighborhoods int
                );
            `;

            // Analysis runs table
            const analysisRunsSchema = `
                CREATE TABLE IF NOT EXISTS analysis_runs (
                    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
                    run_date timestamp DEFAULT now(),
                    analysis_type text,
                    criteria jsonb,
                    total_properties_analyzed int,
                    undervalued_found int,
                    new_listings_added int,
                    descriptions_enhanced int,
                    duration_minutes int,
                    errors jsonb,
                    completed boolean DEFAULT true
                );
            `;

            // Execute schema creation
            const schemas = [enhancedListingsSchema, marketAnalysisSchema, analysisRunsSchema];
            
            for (const schema of schemas) {
                try {
                    await this.supabase.rpc('exec_sql', { sql: schema });
                    console.log('✅ Schema created/updated');
                } catch (error) {
                    console.warn('⚠️ Schema may already exist:', error.message);
                }
            }

            // Create enhanced indexes
            const indexes = [
                'CREATE INDEX IF NOT EXISTS idx_listings_analysis_date ON listings(analysis_date);',
                'CREATE INDEX IF NOT EXISTS idx_listings_score_desc ON listings(score DESC);',
                'CREATE INDEX IF NOT EXISTS idx_listings_percent_below ON listings(percent_below_market DESC);',
                'CREATE INDEX IF NOT EXISTS idx_listings_has_description ON listings(has_description);',
                'CREATE INDEX IF NOT EXISTS idx_market_analysis_date ON market_analysis(analysis_date);',
                'CREATE INDEX IF NOT EXISTS idx_analysis_runs_date ON analysis_runs(run_date);'
            ];

            for (const indexSql of indexes) {
                try {
                    await this.supabase.rpc('exec_sql', { sql: indexSql });
                    console.log('✅ Index created');
                } catch (error) {
                    console.warn('⚠️ Index may already exist:', error.message);
                }
            }

            console.log('✅ Enhanced database setup complete');

        } catch (error) {
            console.error('❌ Database setup error:', error.message);
            console.log('\n💡 Manual setup required. Run this SQL in your Supabase dashboard:');
            console.log(`
-- Enhanced listings table
CREATE TABLE IF NOT EXISTS listings (
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    address text,
    price text,
    beds text,
    sqft text,
    zip text,
    link text,
    score int,
    percent_below_market real,
    warning_tags text[],
    created_at timestamp DEFAULT now(),
    expected_price real,
    days_on_market int,
    distress_signals text[],
    comparison_level text,
    reasoning text,
    has_description boolean DEFAULT false,
    description_snippet text,
    analysis_date timestamp DEFAULT now()
);

-- Market analysis table
CREATE TABLE IF NOT EXISTS market_analysis (
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    analysis_date timestamp DEFAULT now(),
    analysis_type text,
    city_averages jsonb,
    zip_averages jsonb,
    neighborhood_averages jsonb,
    total_cities int,
    total_zips int,
    total_neighborhoods int
);

-- Analysis runs table
CREATE TABLE IF NOT EXISTS analysis_runs (
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    run_date timestamp DEFAULT now(),
    analysis_type text,
    criteria jsonb,
    total_properties_analyzed int,
    undervalued_found int,
    new_listings_added int,
    descriptions_enhanced int,
    duration_minutes int,
    errors jsonb,
    completed boolean DEFAULT true
);
            `);
        }
    }

    /**
     * Clean up old analysis data
     */
    async cleanupOldData(daysOld = 60) {
        try {
            const cutoffDate = new Date();
            cutoffDate.setDate(cutoffDate.getDate() - daysOld);

            // Clean old listings
            const { data: oldListings, error: listingsError } = await this.supabase
                .from('listings')
                .delete()
                .lt('analysis_date', cutoffDate.toISOString())
                .select();

            // Clean old market analysis
            const { data: oldAnalysis, error: analysisError } = await this.supabase
                .from('market_analysis')
                .delete()
                .lt('analysis_date', cutoffDate.toISOString())
                .select();

            if (listingsError || analysisError) {
                throw new Error(`Cleanup errors: ${listingsError?.message}, ${analysisError?.message}`);
            }

            console.log(`🧹 Cleanup complete: ${(oldListings || []).length} old listings, ${(oldAnalysis || []).length} old analyses removed`);
            return (oldListings || []).length + (oldAnalysis || []).length;
        } catch (error) {
            console.error('❌ Error during cleanup:', error.message);
            return 0;
        }
    }
}

// CLI interface
async function main() {
    const args = process.argv.slice(2);
    
    const supabaseUrl = process.env.SUPABASE_URL;
    const supabaseKey = process.env.SUPABASE_ANON_KEY;
    
    if (!supabaseUrl || !supabaseKey) {
        console.error('❌ Missing Supabase environment variables');
        console.error('   Set SUPABASE_URL and SUPABASE_ANON_KEY in .env file');
        process.exit(1);
    }

    const tracker = new HybridPropertyTracker(supabaseUrl, supabaseKey);

    if (args.includes('--setup')) {
        await tracker.setupEnhancedDatabase();
        return;
    }

    if (args.includes('--cleanup')) {
        const days = parseInt(args[args.indexOf('--cleanup') + 1]) || 60;
        await tracker.cleanupOldData(days);
        return;
    }

    if (args.includes('--metrics')) {
        const metrics = await tracker.getAnalysisMetrics();
        console.log('📊 Analysis Metrics:');
        console.log(JSON.stringify(metrics, null, 2));
        return;
    }

    if (args.includes('--distress')) {
        const properties = await tracker.getPropertiesWithDistressSignals();
        console.log(`🚨 Properties with distress signals: ${properties.length}`);
        properties.slice(0, 10).forEach((prop, i) => {
            console.log(`${i + 1}. ${prop.address} - ${prop.price} (Signals: ${prop.distress_signals.join(', ')})`);
        });
        return;
    }

    if (args.includes('--trends')) {
        const trends = await tracker.getMarketTrends();
        console.log(`📈 Market trends (last ${trends.length} analyses):`);
        trends.forEach(trend => {
            console.log(`${trend.analysis_date}: ${trend.total_cities} cities, ${trend.total_zips} ZIP codes analyzed`);
        });
        return;
    }

    // Default: run weekly analysis
    console.log('🗽 Starting weekly hybrid analysis...');
    const results = await tracker.runWeeklyAnalysis();
    
    console.log('\n🎉 Weekly hybrid analysis completed successfully!');
    return results;
}

// Run if executed directly
if (require.main === module) {
    try {
        require('dotenv').config();
    } catch (error) {
        // dotenv not installed
    }
    
    main().catch(console.error);
}

module.exports = HybridPropertyTracker;
