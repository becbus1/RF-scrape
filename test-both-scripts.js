// test-both-scripts.js
// Manual test runner for both sales and rentals scripts
require('dotenv').config();

const SalesAnalyzer = require('./biweekly-streeteasy-sales.js');
const RentalAnalyzer = require('./biweekly-streeteasy-rentals.js');

class TestBothScripts {
    constructor() {
        console.log('🧪 MANUAL TEST MODE: Both Sales and Rentals');
        console.log('⚡ This will override delays and run both scripts in sequence');
        console.log('='.repeat(70));
    }

    async runBothScripts() {
        const startTime = new Date();
        console.log(`🚀 Starting manual test at ${startTime.toISOString()}\n`);

        try {
            // Step 1: Run Sales Analysis (no delay needed)
            console.log('💰 STEP 1: Running Sales Analysis...');
            const salesAnalyzer = new SalesAnalyzer();
            
            // Override the bulk load check for testing
            salesAnalyzer.initialBulkLoad = false;
            
            const salesResults = await salesAnalyzer.runBiWeeklySalesRefresh();
            console.log(`✅ Sales complete: ${salesResults.summary?.savedToDatabase || 0} sales found\n`);

            // Step 2: Wait a bit between scripts
            console.log('⏰ Waiting eight minutes seconds between sales and rentals...');
            await this.delay(8 * 60 * 1000);

            // Step 3: Run Rentals Analysis (override deployment delay)
            console.log('🏠 STEP 2: Running Rentals Analysis...');
            const rentalAnalyzer = new RentalAnalyzer();
            
            // Override the deployment time to skip 15-minute wait
            rentalAnalyzer.deployTime = new Date().getTime() - (20 * 60 * 1000); // 20 minutes ago
            
            const rentalResults = await rentalAnalyzer.runBiWeeklyRentalRefresh();
            console.log(`✅ Rentals complete: ${rentalResults.summary?.savedToDatabase || 0} rentals found\n`);

            // Final summary
            const endTime = new Date();
            const totalDuration = (endTime - startTime) / 1000 / 60;

            console.log('\n🎉 MANUAL TEST COMPLETE');
            console.log('='.repeat(70));
            console.log(`⏱️ Total duration: ${totalDuration.toFixed(1)} minutes`);
            console.log(`💰 Sales found: ${salesResults.summary?.savedToDatabase || 0}`);
            console.log(`🏠 Rentals found: ${rentalResults.summary?.savedToDatabase || 0}`);
            console.log(`📞 Total API calls: ${(salesResults.summary?.apiCallsUsed || 0) + (rentalResults.summary?.apiCallsUsed || 0)}`);

            if (salesResults.summary?.apiCallsSaved || rentalResults.summary?.apiCallsSaved) {
                const totalSaved = (salesResults.summary?.apiCallsSaved || 0) + (rentalResults.summary?.apiCallsSaved || 0);
                console.log(`⚡ API calls saved: ${totalSaved} (smart deduplication working!)`);
            }

            console.log('\n🔍 Next steps:');
            console.log('   1. Check your Supabase tables for new data');
            console.log('   2. Deploy individual scripts with Railway cron for production');
            console.log('   3. Each script will run independently on its schedule');

            // Keep the process alive for a moment to see results
            console.log('\n⏰ Keeping process alive for eight minutes seconds to view results...');
            await this.delay(8 * 60 * 1000);
            
            console.log('✅ Test complete - process will now exit');
            process.exit(0);

        } catch (error) {
            console.error('💥 Manual test failed:', error.message);
            console.error('Stack trace:', error.stack);
            process.exit(1);
        }
    }

    delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

// CLI interface
async function main() {
    const args = process.argv.slice(2);
    
    if (!process.env.RAPIDAPI_KEY || !process.env.SUPABASE_URL || !process.env.SUPABASE_ANON_KEY) {
        console.error('❌ Missing required environment variables!');
        console.error('   RAPIDAPI_KEY, SUPABASE_URL, SUPABASE_ANON_KEY required');
        process.exit(1);
    }

    if (args.includes('--help')) {
        console.log('🧪 Manual Test Runner for Both Scripts');
        console.log('');
        console.log('Usage:');
        console.log('  node test-both-scripts.js          # Run both sales and rentals');
        console.log('  node test-both-scripts.js --help   # Show this help');
        console.log('');
        console.log('This script:');
        console.log('  - Overrides deployment delays for immediate testing');
        console.log('  - Runs sales script first, then rentals script');
        console.log('  - Shows combined results and API usage');
        console.log('  - Keeps process alive to view results');
        return;
    }

    console.log('🧪 MANUAL TEST: Both Sales and Rentals Scripts');
    console.log('⚠️  This is for testing only - production uses separate cron jobs');
    console.log('');

    const tester = new TestBothScripts();
    await tester.runBothScripts();
}

// Export for use in other modules
module.exports = TestBothScripts;

// Run if executed directly
if (require.main === module) {
    main().catch(error => {
        console.error('💥 Test runner crashed:', error);
        process.exit(1);
    });
}
