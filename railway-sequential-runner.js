// railway-sequential-runner.js
// DEPLOYMENT FIX: Run both sales and rentals scripts sequentially on Railway
// This ensures both scripts execute in the correct order with proper delays

require('dotenv').config();

const SalesAnalyzer = require('./biweekly-streeteasy-sales.js');
const RentalAnalyzer = require('./biweekly-streeteasy-rentals.js');

class RailwaySequentialRunner {
    constructor() {
        console.log('🚀 RAILWAY SEQUENTIAL RUNNER: Sales → Rentals');
        console.log('⚡ This runs both scripts in sequence to prevent conflicts');
        console.log('⏰ Built-in delays prevent API rate limit conflicts');
        console.log('='.repeat(70));
    }

    async runBothScriptsSequentially() {
        const startTime = new Date();
        console.log(`🚀 Starting sequential analysis at ${startTime.toISOString()}\n`);

        const results = {
            sales: null,
            rentals: null,
            totalDuration: 0,
            totalApiCalls: 0,
            totalSaved: 0,
            success: false
        };

        try {
            // Step 1: Run Sales Analysis First
            console.log('💰 STEP 1: Running Sales Analysis...');
            console.log('='.repeat(50));
            
            const salesAnalyzer = new SalesAnalyzer();
            
            // Override bulk load check if needed
            if (process.env.INITIAL_BULK_LOAD === 'true') {
                console.log('🚀 BULK LOAD MODE: Processing sales first...');
                salesAnalyzer.initialBulkLoad = true;
            }
            
            const salesResults = await salesAnalyzer.runBiWeeklySalesRefresh();
            results.sales = salesResults;
            
            console.log(`✅ Sales complete: ${salesResults.summary?.savedToDatabase || 0} sales found`);
            console.log(`📞 Sales API calls: ${salesResults.summary?.apiCallsUsed || 0}`);

            // Step 2: Wait Between Scripts (CRITICAL for API rate limits)
            console.log('\n⏰ STEP 2: Waiting 2 minutes between sales and rentals...');
            console.log('🔄 This prevents API conflicts and rate limiting');
            
            // Show countdown
            for (let i = 120; i > 0; i -= 10) {
                console.log(`   ⏰ ${i} seconds remaining...`);
                await this.delay(10000); // 10 second intervals
            }
            console.log('✅ Inter-script delay complete!\n');

            // Step 3: Run Rentals Analysis Second
            console.log('🏠 STEP 3: Running Rentals Analysis...');
            console.log('=' * 50);
            
            const rentalAnalyzer = new RentalAnalyzer();
            
            // Override deployment delay (we already waited 2 minutes)
            rentalAnalyzer.deployTime = new Date().getTime() - (20 * 60 * 1000); // 20 minutes ago
            
            // Override bulk load if needed
            if (process.env.INITIAL_BULK_LOAD === 'true') {
                console.log('🚀 BULK LOAD MODE: Processing rentals second...');
                rentalAnalyzer.initialBulkLoad = true;
            }
            
            const rentalResults = await rentalAnalyzer.runBiWeeklyRentalRefresh();
            results.rentals = rentalResults;
            
            console.log(`✅ Rentals complete: ${rentalResults.summary?.savedToDatabase || 0} rentals found`);
            console.log(`📞 Rentals API calls: ${rentalResults.summary?.apiCallsUsed || 0}`);

            // Step 4: Calculate Final Summary
            const endTime = new Date();
            const totalDuration = (endTime - startTime) / 1000 / 60;

            results.totalDuration = totalDuration;
            results.totalApiCalls = (salesResults.summary?.apiCallsUsed || 0) + (rentalResults.summary?.apiCallsUsed || 0);
            results.totalSaved = (salesResults.summary?.savedToDatabase || 0) + (rentalResults.summary?.savedToDatabase || 0);
            results.success = true;

            // Final Summary
            console.log('\n🎉 RAILWAY SEQUENTIAL ANALYSIS COMPLETE');
            console.log('='.repeat(70));
            console.log(`⏱️ Total duration: ${totalDuration.toFixed(1)} minutes`);
            console.log(`💰 Sales found: ${salesResults.summary?.savedToDatabase || 0}`);
            console.log(`🏠 Rentals found: ${rentalResults.summary?.savedToDatabase || 0}`);
            console.log(`📊 Total properties saved: ${results.totalSaved}`);
            console.log(`📞 Total API calls used: ${results.totalApiCalls}`);

            // API Efficiency Report
            const salesApiSaved = salesResults.summary?.apiCallsSaved || 0;
            const rentalsApiSaved = rentalResults.summary?.apiCallsSaved || 0;
            const totalApiSaved = salesApiSaved + rentalsApiSaved;
            
            if (totalApiSaved > 0) {
                const efficiency = ((totalApiSaved / (results.totalApiCalls + totalApiSaved)) * 100).toFixed(1);
                console.log(`⚡ API efficiency: ${efficiency}% (${totalApiSaved} calls saved through smart caching)`);
            }

            console.log('\n📊 Next Steps:');
            console.log('   1. Check your Supabase tables:');
            console.log(`      - undervalued_sales: ${salesResults.summary?.savedToDatabase || 0} new deals`);
            console.log(`      - undervalued_rentals: ${rentalResults.summary?.savedToDatabase || 0} new deals`);
            console.log('   2. Set up separate Railway cron jobs for production:');
            console.log('      - Sales: node biweekly-streeteasy-sales.js (days 1-8, 15-22)');
            console.log('      - Rentals: node biweekly-streeteasy-rentals.js (days 8-15, 22-29)');
            
            if (process.env.INITIAL_BULK_LOAD === 'true') {
                console.log('\n🎯 BULK LOAD COMPLETE!');
                console.log('📝 IMPORTANT: Set INITIAL_BULK_LOAD=false in Railway to switch to maintenance mode');
            }

            return results;

        } catch (error) {
            console.error('💥 Sequential analysis failed:', error.message);
            console.error('Stack trace:', error.stack);
            
            results.success = false;
            results.error = error.message;
            
            // Try to save partial results
            if (results.sales && !results.rentals) {
                console.log('⚠️ Sales completed but rentals failed');
                console.log('💡 You can run rentals separately: node biweekly-streeteasy-rentals.js');
            }
            
            throw error;
        }
    }

    delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

// Railway Deployment Helper Functions
class RailwayDeploymentHelper {
    static async checkEnvironmentVariables() {
        const required = ['RAPIDAPI_KEY', 'SUPABASE_URL', 'SUPABASE_ANON_KEY'];
        const missing = required.filter(key => !process.env[key]);
        
        if (missing.length > 0) {
            console.error('❌ Missing required Railway environment variables:');
            missing.forEach(key => console.error(`   ${key}`));
            console.error('\n💡 Add these in Railway Dashboard → Variables');
            return false;
        }
        
        console.log('✅ All required environment variables found');
        return true;
    }
    
    static logDeploymentInfo() {
        console.log('🚀 RAILWAY DEPLOYMENT INFO');
        console.log('='.repeat(50));
        console.log(`📅 Deploy time: ${new Date().toISOString()}`);
        console.log(`🔧 Bulk load mode: ${process.env.INITIAL_BULK_LOAD === 'true' ? 'ENABLED' : 'DISABLED'}`);
        console.log(`🗽 Target: NYC Sales & Rentals`);
        console.log(`⚡ Smart deduplication: ENABLED`);
        console.log(`📊 Expected duration: ${process.env.INITIAL_BULK_LOAD === 'true' ? '2-4 hours' : '10-30 minutes'}`);
        console.log('');
    }
    
    static async setupGracefulShutdown(runner) {
        process.on('SIGTERM', () => {
            console.log('\n📥 Railway shutdown signal received');
            console.log('💾 Allowing current operations to complete...');
            // Let current operations finish naturally
        });
        
        process.on('SIGINT', () => {
            console.log('\n📥 Interrupt signal received');
            console.log('💾 Graceful shutdown initiated...');
            process.exit(0);
        });
    }
}

// Main execution for Railway
async function main() {
    const args = process.argv.slice(2);
    
    try {
        // Step 1: Check environment
        RailwayDeploymentHelper.logDeploymentInfo();
        
        const envOk = await RailwayDeploymentHelper.checkEnvironmentVariables();
        if (!envOk) {
            process.exit(1);
        }

        // Step 2: Handle CLI arguments
        if (args.includes('--help')) {
            console.log('🚀 Railway Sequential Runner');
            console.log('');
            console.log('Usage:');
            console.log('  node railway-sequential-runner.js           # Run both sales and rentals');
            console.log('  node railway-sequential-runner.js --help    # Show this help');
            console.log('');
            console.log('Environment Variables:');
            console.log('  INITIAL_BULK_LOAD=true   # Enable bulk load mode (process all neighborhoods)');
            console.log('  INITIAL_BULK_LOAD=false  # Regular bi-weekly maintenance mode');
            console.log('');
            console.log('Railway Deployment:');
            console.log('  1. Set environment variables in Railway Dashboard');
            console.log('  2. Deploy this file as your main entry point');
            console.log('  3. Both sales and rentals will run sequentially');
            return;
        }

        // Step 3: Run sequential analysis
        const runner = new RailwaySequentialRunner();
        RailwayDeploymentHelper.setupGracefulShutdown(runner);
        
        const results = await runner.runBothScriptsSequentially();
        
        console.log('\n✅ Railway deployment completed successfully!');
        console.log(`🎯 Total properties found: ${results.totalSaved}`);
        
        // Keep process alive briefly to see final results
        console.log('\n⏰ Keeping process alive for 30 seconds to view results...');
        await runner.delay(30000);
        
        console.log('✅ Analysis complete - Railway deployment successful');
        process.exit(0);

    } catch (error) {
        console.error('💥 Railway deployment failed:', error.message);
        console.error('\n🔧 Troubleshooting:');
        console.error('   1. Check Railway environment variables');
        console.error('   2. Verify Supabase connection');
        console.error('   3. Check RapidAPI key validity');
        console.error('   4. Review Railway logs for detailed errors');
        
        process.exit(1);
    }
}

// Export for testing
module.exports = { RailwaySequentialRunner, RailwayDeploymentHelper };

// Run if executed directly (Railway entry point)
if (require.main === module) {
    main().catch(error => {
        console.error('💥 Railway sequential runner crashed:', error);
        process.exit(1);
    });
}
