// supabase-integration.js
// Daily NYC property scraper that stores undervalued properties in Supabase

const { createClient } = require('@supabase/supabase-js');
const UndervaluedPropertyFinder = require('./undervalued-property-finder.js');

class NYCPropertyTracker {
    constructor(supabaseUrl, supabaseKey) {
        this.supabase = createClient(supabaseUrl, supabaseKey);
        this.finder = new UndervaluedPropertyFinder();
        
        // NYC-focused search locations
        this.nycLocations = [
            'Manhattan, NY',
            'Brooklyn, NY', 
            'Queens, NY',
            'Bronx, NY',
            'Staten Island, NY'
        ];
    }

    /**
     * Run daily scraping for all NYC boroughs
     */
    async runDailyNYCScrape(locations = this.nycLocations, options = {}) {
        console.log(`🗽 Starting daily NYC property scrape at ${new Date().toISOString()}`);
        console.log(`📍 NYC Locations: ${locations.join(', ')}`);

        const defaultOptions = {
            minDiscountPercent: 15,  // At least 15% below market
            maxDaysOnMarket: 90,     // Listed within 90 days
            maxPrice: 3000000,       // Under $3M for NYC
            limit: 350,              // Check all available listings
            ...options
        };

        const allResults = [];
        const summary = {
            startTime: new Date().toISOString(),
            locations: locations.length,
            totalListingsAnalyzed: 0,
            totalUndervaluedFound: 0,
            newListingsAdded: 0,
            errors: []
        };

        for (const location of locations) {
            try {
                console.log(`\n🔍 Scraping ${location}...`);
                
                const results = await this.finder.findUndervaluedProperties(location, defaultOptions);
                
                summary.totalListingsAnalyzed += results.totalListings;
                summary.totalUndervaluedFound += results.undervaluedCount;

                if (results.undervaluedProperties.length > 0) {
                    const newListings = await this.saveNewNYCListings(results.undervaluedProperties, location);
                    summary.newListingsAdded += newListings;
                    allResults.push(...results.undervaluedProperties);
                }

                console.log(`✅ ${location}: ${results.undervaluedCount} undervalued properties found`);

            } catch (error) {
                console.error(`❌ Error scraping ${location}:`, error.message);
                summary.errors.push({ location, error: error.message });
            }

            // Rate limiting between NYC boroughs
            await new Promise(resolve => setTimeout(resolve, 5000));
        }

        summary.endTime = new Date().toISOString();
        summary.duration = new Date(summary.endTime) - new Date(summary.startTime);

        // Save summary to database
        await this.saveDailySummary(summary);

        console.log('\n🗽 DAILY NYC SCRAPE COMPLETE');
        console.log('='.repeat(50));
        console.log(`📋 NYC locations scraped: ${summary.locations}`);
        console.log(`🔍 Total listings analyzed: ${summary.totalListingsAnalyzed}`);
        console.log(`🎯 Undervalued properties found: ${summary.totalUndervaluedFound}`);
        console.log(`💾 New listings added: ${summary.newListingsAdded}`);
        console.log(`⏱️ Duration: ${Math.round(summary.duration / 1000 / 60)} minutes`);
        
        if (summary.errors.length > 0) {
            console.log(`❌ Errors: ${summary.errors.length}`);
            summary.errors.forEach(err => {
                console.log(`   - ${err.location}: ${err.error}`);
            });
        }

        return { summary, allResults };
    }

    /**
     * Save new NYC listings to Supabase, avoiding duplicates
     */
    async saveNewNYCListings(properties, location) {
        console.log(`💾 Saving ${properties.length} NYC properties to database...`);

        let newCount = 0;

        for (const property of properties) {
            try {
                // Check if property already exists (by address + price)
                const { data: existing } = await this.supabase
                    .from('listings')
                    .select('id')
                    .eq('address', property.address)
                    .eq('price', `$${property.price.toLocaleString()}`)
                    .single();

                if (existing) {
                    console.log(`   ⏭️ Skipping duplicate: ${property.address}`);
                    continue;
                }

                // Prepare data for database (matching your exact schema)
                const listingData = {
                    address: property.address,
                    price: `$${property.price.toLocaleString()}`,
                    beds: property.beds?.toString() || null,
                    sqft: property.sqft?.toString() || null,
                    zip: property.zip || null,
                    link: property.url || null,
                    score: property.score,
                    percent_below_market: property.percentBelowMarket,
                    warning_tags: property.warningTags || []
                };

                // Insert into database
                const { error } = await this.supabase
                    .from('listings')
                    .insert([listingData]);

                if (error) {
                    console.error(`❌ Error saving ${property.address}:`, error.message);
                } else {
                    console.log(`✅ Saved: ${property.address} (${property.percentBelowMarket.toFixed(1)}% below market, Score: ${property.score})`);
                    newCount++;
                }

            } catch (error) {
                console.error(`❌ Error processing ${property.address}:`, error.message);
            }
        }

        console.log(`💾 Added ${newCount} new NYC listings to database`);
        return newCount;
    }

    /**
     * Save daily scrape summary for tracking performance
     */
    async saveDailySummary(summary) {
        try {
            // Create scrape_runs table if it doesn't exist (run setup first)
            const { error } = await this.supabase
                .from('scrape_runs')
                .insert([{
                    run_date: summary.startTime,
                    locations_count: summary.locations,
                    total_listings_analyzed int,
                    undervalued_found int,
                    new_listings_added int,
                    duration_minutes int,
                    errors jsonb,
                    created_at timestamp DEFAULT now()
                );
                `
            });

            if (runsError) {
                console.warn('⚠️ Could not create scrape_runs table:', runsError.message);
                console.log('💡 You may need to run this SQL manually in Supabase:');
                console.log(`
CREATE TABLE IF NOT EXISTS scrape_runs (
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    run_date timestamp DEFAULT now(),
    locations_count int,
    total_listings_analyzed int,
    undervalued_found int,
    new_listings_added int,
    duration_minutes int,
    errors jsonb,
    created_at timestamp DEFAULT now()
);
                `);
            } else {
                console.log('✅ scrape_runs table created successfully');
            }

            // Create indexes for better performance
            const indexes = [
                'CREATE INDEX IF NOT EXISTS idx_listings_score ON listings(score);',
                'CREATE INDEX IF NOT EXISTS idx_listings_zip ON listings(zip);',
                'CREATE INDEX IF NOT EXISTS idx_listings_created_at ON listings(created_at);',
                'CREATE INDEX IF NOT EXISTS idx_listings_percent_below ON listings(percent_below_market);'
            ];

            for (const indexSql of indexes) {
                try {
                    await this.supabase.rpc('exec_sql', { sql: indexSql });
                    console.log('✅ Created index');
                } catch (error) {
                    console.warn('⚠️ Index may already exist:', error.message);
                }
            }

            console.log('✅ NYC database setup complete');

        } catch (error) {
            console.error('❌ Database setup error:', error.message);
            console.log('\n💡 Manual setup required. Run this SQL in your Supabase dashboard:');
            console.log(`
-- Your existing table (should already exist)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

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
  created_at timestamp DEFAULT now()
);

-- New table for tracking scrapes
CREATE TABLE IF NOT EXISTS scrape_runs (
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    run_date timestamp DEFAULT now(),
    locations_count int,
    total_listings_analyzed int,
    undervalued_found int,
    new_listings_added int,
    duration_minutes int,
    errors jsonb,
    created_at timestamp DEFAULT now()
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_listings_score ON listings(score);
CREATE INDEX IF NOT EXISTS idx_listings_zip ON listings(zip);
CREATE INDEX IF NOT EXISTS idx_listings_created_at ON listings(created_at);
CREATE INDEX IF NOT EXISTS idx_listings_percent_below ON listings(percent_below_market);
            `);
        }
    }
}

// CLI interface for running NYC scrapes
async function main() {
    const args = process.argv.slice(2);
    
    // Environment variables for Supabase
    const supabaseUrl = process.env.SUPABASE_URL;
    const supabaseKey = process.env.SUPABASE_ANON_KEY;
    
    if (!supabaseUrl || !supabaseKey) {
        console.error('❌ Missing Supabase environment variables:');
        console.error('   SUPABASE_URL and SUPABASE_ANON_KEY must be set');
        console.error('\n📋 Setup instructions:');
        console.error('   1. Create .env file with:');
        console.error('      SUPABASE_URL=your_supabase_url');
        console.error('      SUPABASE_ANON_KEY=your_supabase_anon_key');
        console.error('   2. Run: npm install dotenv @supabase/supabase-js');
        console.error('   3. Add to your script: require("dotenv").config()');
        process.exit(1);
    }

    const tracker = new NYCPropertyTracker(supabaseUrl, supabaseKey);

    if (args.includes('--setup')) {
        await tracker.setupNYCDatabase();
        return;
    }

    if (args.includes('--cleanup')) {
        const days = parseInt(args[args.indexOf('--cleanup') + 1]) || 30;
        await tracker.cleanupOldListings(days);
        return;
    }

    if (args.includes('--stats')) {
        const stats = await tracker.getNYCMarketStats();
        console.log('🗽 NYC Market Statistics:');
        console.log(JSON.stringify(stats, null, 2));
        return;
    }

    if (args.includes('--latest')) {
        const limit = parseInt(args[args.indexOf('--latest') + 1]) || 10;
        const properties = await tracker.getLatestNYCProperties(limit);
        console.log(`🗽 Latest ${properties.length} undervalued NYC properties:`);
        properties.forEach((prop, i) => {
            console.log(`${i + 1}. ${prop.address} - ${prop.price} (Score: ${prop.score})`);
        });
        return;
    }

    if (args.includes('--top-deals')) {
        const limit = parseInt(args[args.indexOf('--top-deals') + 1]) || 20;
        const deals = await tracker.getTopNYCDeals(limit);
        console.log(`🏆 Top ${deals.length} NYC deals:`);
        deals.forEach((deal, i) => {
            console.log(`${i + 1}. ${deal.address} - ${deal.price} (Score: ${deal.score}, ${deal.percent_below_market}% below market)`);
        });
        return;
    }

    if (args.includes('--zip')) {
        const zipCode = args[args.indexOf('--zip') + 1];
        if (!zipCode) {
            console.error('❌ Please provide a ZIP code: --zip 10001');
            return;
        }
        const properties = await tracker.getPropertiesByNYCZip(zipCode);
        console.log(`🗽 Properties in NYC ZIP ${zipCode}:`);
        properties.forEach((prop, i) => {
            console.log(`${i + 1}. ${prop.address} - ${prop.price} (Score: ${prop.score})`);
        });
        return;
    }

    // Default: run daily NYC scrape
    console.log('🗽 Starting daily NYC property scrape...');
    const results = await tracker.runDailyNYCScrape();
    
    console.log('\n🎉 Daily NYC scrape completed successfully!');
    return results;
}

// Example usage and testing
async function nycExample() {
    console.log('🗽 NYC Supabase Integration Example\n');
    
    // Mock environment for example
    const mockSupabaseUrl = 'https://your-project.supabase.co';
    const mockSupabaseKey = 'your-anon-key';
    
    console.log('🔧 NYC-focused setup:');
    console.log(`
// 1. Install dependencies
npm install @supabase/supabase-js dotenv

// 2. Create .env file
SUPABASE_URL=${mockSupabaseUrl}
SUPABASE_ANON_KEY=${mockSupabaseKey}

// 3. Your Supabase SQL schema (already perfect!)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE listings (
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
    created_at timestamp DEFAULT now()
);

// 4. NYC usage in your code
require('dotenv').config();
const tracker = new NYCPropertyTracker(
    process.env.SUPABASE_URL, 
    process.env.SUPABASE_ANON_KEY
);

// Run daily NYC scrape
await tracker.runDailyNYCScrape();

// Get latest NYC properties
const latest = await tracker.getLatestNYCProperties(20);

// Get top NYC deals
const topDeals = await tracker.getTopNYCDeals(10);

// Clean up old data
await tracker.cleanupOldListings(30);
    `);

    console.log('\n📋 NYC CLI Commands:');
    console.log('node supabase-integration.js                    # Run daily NYC scrape');
    console.log('node supabase-integration.js --setup           # Setup database');
    console.log('node supabase-integration.js --latest 20       # Get latest 20 NYC properties');
    console.log('node supabase-integration.js --top-deals 10    # Get top 10 NYC deals');
    console.log('node supabase-integration.js --zip 10001       # Get properties in ZIP code');
    console.log('node supabase-integration.js --cleanup 30      # Clean properties older than 30 days');
    console.log('node supabase-integration.js --stats           # Get NYC market statistics');
}

// Run if executed directly
if (require.main === module) {
    // Load environment variables if available
    try {
        require('dotenv').config();
    } catch (error) {
        // dotenv not installed, continue without it
    }
    
    if (process.argv.includes('--example')) {
        nycExample();
    } else {
        main().catch(console.error);
    }
}

module.exports = NYCPropertyTracker;d: summary.totalListingsAnalyzed,
                    undervalued_found: summary.totalUndervaluedFound,
                    new_listings_added: summary.newListingsAdded,
                    duration_minutes: Math.round(summary.duration / 1000 / 60),
                    errors: summary.errors
                }]);

            if (error) {
                console.error('❌ Error saving summary:', error.message);
            } else {
                console.log('✅ Daily summary saved to database');
            }
        } catch (error) {
            console.error('❌ Error saving summary:', error.message);
        }
    }

    /**
     * Get latest undervalued NYC properties from database
     */
    async getLatestNYCProperties(limit = 50, minScore = 40) {
        try {
            const { data, error } = await this.supabase
                .from('listings')
                .select('*')
                .gte('score', minScore)
                .order('created_at', { ascending: false })
                .limit(limit);

            if (error) {
                throw error;
            }

            return data;
        } catch (error) {
            console.error('❌ Error fetching NYC properties:', error.message);
            return [];
        }
    }

    /**
     * Get properties by NYC ZIP code
     */
    async getPropertiesByNYCZip(zipCode, limit = 20) {
        try {
            const { data, error } = await this.supabase
                .from('listings')
                .select('*')
                .eq('zip', zipCode)
                .order('score', { ascending: false })
                .limit(limit);

            if (error) {
                throw error;
            }

            return data;
        } catch (error) {
            console.error('❌ Error fetching NYC properties by ZIP:', error.message);
            return [];
        }
    }

    /**
     * Get top scoring properties across all NYC
     */
    async getTopNYCDeals(limit = 20) {
        try {
            const { data, error } = await this.supabase
                .from('listings')
                .select('*')
                .gte('score', 70) // Only high-scoring deals
                .order('score', { ascending: false })
                .limit(limit);

            if (error) {
                throw error;
            }

            console.log(`🏆 Found ${data.length} top NYC deals (Score 70+)`);
            return data;
        } catch (error) {
            console.error('❌ Error fetching top NYC deals:', error.message);
            return [];
        }
    }

    /**
     * Clean up old listings (older than 30 days)
     */
    async cleanupOldListings(daysOld = 30) {
        try {
            const cutoffDate = new Date();
            cutoffDate.setDate(cutoffDate.getDate() - daysOld);

            const { data, error } = await this.supabase
                .from('listings')
                .delete()
                .lt('created_at', cutoffDate.toISOString())
                .select();

            if (error) {
                throw error;
            }

            console.log(`🧹 Cleaned up ${data.length} old NYC listings (older than ${daysOld} days)`);
            return data.length;
        } catch (error) {
            console.error('❌ Error cleaning up old listings:', error.message);
            return 0;
        }
    }

    /**
     * Get NYC market statistics
     */
    async getNYCMarketStats() {
        try {
            // Get basic stats
            const { data: allListings, error } = await this.supabase
                .from('listings')
                .select('score, percent_below_market, zip');

            if (error) {
                throw error;
            }

            if (!allListings || allListings.length === 0) {
                return { message: 'No listings found in database' };
            }

            // Calculate statistics
            const stats = {
                totalListings: allListings.length,
                avgScore: Math.round(allListings.reduce((sum, item) => sum + item.score, 0) / allListings.length),
                avgPercentBelowMarket: Math.round(allListings.reduce((sum, item) => sum + item.percent_below_market, 0) / allListings.length * 10) / 10,
                scoreDistribution: {
                    excellent: allListings.filter(item => item.score >= 80).length,
                    good: allListings.filter(item => item.score >= 60 && item.score < 80).length,
                    fair: allListings.filter(item => item.score >= 40 && item.score < 60).length,
                    poor: allListings.filter(item => item.score < 40).length
                }
            };

            // Top ZIP codes
            const zipCounts = {};
            allListings.forEach(item => {
                if (item.zip) {
                    zipCounts[item.zip] = (zipCounts[item.zip] || 0) + 1;
                }
            });

            stats.topZipCodes = Object.entries(zipCounts)
                .sort((a, b) => b[1] - a[1])
                .slice(0, 10)
                .map(([zip, count]) => ({ zip, count }));

            return stats;
        } catch (error) {
            console.error('❌ Error getting NYC market stats:', error.message);
            return null;
        }
    }

    /**
     * Setup database schema for NYC property tracking
     */
    async setupNYCDatabase() {
        console.log('🔧 Setting up NYC property database schema...');

        try {
            // Note: Your listings table already exists, but let's create the scrape_runs table
            const { error: runsError } = await this.supabase.rpc('exec_sql', {
                sql: `
                CREATE TABLE IF NOT EXISTS scrape_runs (
                    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
                    run_date timestamp DEFAULT now(),
                    locations_count int,
                    total_listings_analyze
