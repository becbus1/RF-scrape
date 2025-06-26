// railway-sequential-runner.js
// UPDATED: Now focuses on rent-stabilized finder only with priority-based neighborhood targeting
// Sales and rentals scrapers are disabled

require('dotenv').config();

// Import rent-stabilized system instead of sales/rentals
const RentStabilizedUndervaluedDetector = require('./rent-stabilized-undervalued-system.js');

class RailwayRentStabilizedRunner {
   constructor() {
       console.log('🏠 RAILWAY RENT-STABILIZED FINDER');
       console.log('🎯 Focus: Undervalued rent-stabilized apartments only');
       console.log('🧠 Smart targeting: Priority-based neighborhood selection');
       console.log('❌ Sales & rentals scrapers: DISABLED');
       console.log('='.repeat(70));
   }

   async runRentStabilizedAnalysis() {
       const startTime = new Date();
       console.log(`🚀 Starting rent-stabilized analysis at ${startTime.toISOString()}\n`);

       const results = {
           rentStabilized: null,
           totalDuration: 0,
           success: false,
           mode: 'rent_stabilized_only'
       };

       try {
           // Run Rent-Stabilized Analysis
           console.log('🏠 STEP 1: Running Rent-Stabilized + Undervaluation Analysis...');
           console.log('='.repeat(60));
           
           const detector = new RentStabilizedUndervaluedDetector();
           
           // Get neighborhoods using priority-based targeting
           const targetNeighborhoods = this.getTargetNeighborhoods();
           
           // Configure for production
           const searchOptions = {
               neighborhoods: targetNeighborhoods,
               maxListingsPerNeighborhood: process.env.INITIAL_BULK_LOAD === 'true' ? 200 : 100,
               testMode: false
           };
           
           console.log(`🎯 Target neighborhoods (${targetNeighborhoods.length}): ${targetNeighborhoods.join(', ')}`);
           console.log(`📊 Max listings per neighborhood: ${searchOptions.maxListingsPerNeighborhood}`);
           console.log(`🧪 Bulk load mode: ${process.env.INITIAL_BULK_LOAD === 'true' ? 'ENABLED' : 'DISABLED'}\n`);
           
           const rentStabilizedResults = await detector.findUndervaluedRentStabilizedListings(searchOptions);
           results.rentStabilized = rentStabilizedResults;
           
           // Calculate duration
           const endTime = new Date();
           results.totalDuration = Math.round((endTime - startTime) / 1000 / 60); // minutes
           
           console.log(`\n✅ Rent-stabilized analysis complete!`);
           console.log(`🎯 Undervalued rent-stabilized listings found: ${rentStabilizedResults.undervaluedStabilizedFound}`);
           console.log(`⏱️ Total duration: ${results.totalDuration} minutes`);

           results.success = true;
           
           // Log summary for Railway dashboard
           this.logRailwaySummary(results);
           
           return results;

       } catch (error) {
           console.error('💥 Rent-stabilized analysis failed:', error.message);
           console.error('Stack trace:', error.stack);
           
           results.success = false;
           results.error = error.message;
           
           throw error;
       }
   }

   /**
    * PRIORITY-BASED NEIGHBORHOOD TARGETING
    * Simple, effective targeting of high-value rent-stabilized areas
    */
   getTargetNeighborhoods() {
       // Override for testing single neighborhood
       if (process.env.TEST_NEIGHBORHOOD) {
           console.log(`🧪 Testing mode: Only targeting ${process.env.TEST_NEIGHBORHOOD}`);
           return [process.env.TEST_NEIGHBORHOOD];
       }
       
       // Override with manual neighborhoods if specified
       if (process.env.MANUAL_NEIGHBORHOODS) {
           const manualNeighborhoods = process.env.MANUAL_NEIGHBORHOODS.split(',').map(n => n.trim());
           console.log(`🎯 Manual override: Targeting ${manualNeighborhoods.length} specified neighborhoods`);
           return manualNeighborhoods;
       }
       
       console.log('🎯 Using priority-based neighborhood targeting...');
       
       const highPriorityNeighborhoods = {
           // Manhattan (highest priority - most rent-stabilized + highest value)
           manhattan: [
               'east-village', 'lower-east-side', 'chinatown', 'financial-district',
               'west-village', 'greenwich-village', 'soho', 'nolita', 'tribeca',
               'chelsea', 'gramercy', 'murray-hill', 'kips-bay', 'flatiron',
               'upper-east-side', 'upper-west-side', 'hells-kitchen', 'midtown-east'
           ],
           
           // Brooklyn (second priority - many rent-stabilized, good value)
           brooklyn: [
               'williamsburg', 'dumbo', 'brooklyn-heights', 'cobble-hill',
               'carroll-gardens', 'park-slope', 'fort-greene', 'boerum-hill',
               'red-hook', 'prospect-heights', 'crown-heights', 'bedford-stuyvesant',
               'greenpoint', 'bushwick'
           ],
           
           // Queens (third priority - lots of rent-stabilized, emerging value)
           queens: [
               'long-island-city', 'astoria', 'sunnyside', 'woodside',
               'jackson-heights', 'elmhurst', 'forest-hills', 'ridgewood'
           ],
           
           // Bronx (fourth priority - many rent-stabilized, most affordable)
           bronx: [
               'mott-haven', 'concourse', 'fordham', 'university-heights',
               'morrisania', 'melrose'
           ]
       };

       // Focus on specific borough if requested
       if (process.env.FOCUS_BOROUGH) {
           const focusBorough = process.env.FOCUS_BOROUGH.toLowerCase();
           if (highPriorityNeighborhoods[focusBorough]) {
               const boroughNeighborhoods = highPriorityNeighborhoods[focusBorough];
               console.log(`🎯 Borough focus: ${focusBorough} (${boroughNeighborhoods.length} neighborhoods)`);
               return boroughNeighborhoods;
           }
       }

       if (process.env.INITIAL_BULK_LOAD === 'true') {
           // Bulk mode: All priority neighborhoods
           const allNeighborhoods = [
               ...highPriorityNeighborhoods.manhattan,
               ...highPriorityNeighborhoods.brooklyn,
               ...highPriorityNeighborhoods.queens,
               ...highPriorityNeighborhoods.bronx
           ];
           
           console.log(`🚀 Bulk load mode: Targeting all ${allNeighborhoods.length} priority neighborhoods`);
           console.log(`   📍 Manhattan: ${highPriorityNeighborhoods.manhattan.length} neighborhoods`);
           console.log(`   📍 Brooklyn: ${highPriorityNeighborhoods.brooklyn.length} neighborhoods`);
           console.log(`   📍 Queens: ${highPriorityNeighborhoods.queens.length} neighborhoods`);
           console.log(`   📍 Bronx: ${highPriorityNeighborhoods.bronx.length} neighborhoods`);
           
           return allNeighborhoods;
       } else {
           // Regular mode: Focus on highest value areas
           const maxNeighborhoods = parseInt(process.env.MAX_NEIGHBORHOODS_REGULAR) || 15;
           
           const selectedNeighborhoods = [
               ...highPriorityNeighborhoods.manhattan.slice(0, 8),  // Top 8 Manhattan
               ...highPriorityNeighborhoods.brooklyn.slice(0, 4),   // Top 4 Brooklyn
               ...highPriorityNeighborhoods.queens.slice(0, 2),     // Top 2 Queens
               ...highPriorityNeighborhoods.bronx.slice(0, 1)       // Top 1 Bronx
           ].slice(0, maxNeighborhoods);
           
           console.log(`⚡ Regular mode: Targeting top ${selectedNeighborhoods.length} priority neighborhoods`);
           console.log(`   🏆 Manhattan: ${highPriorityNeighborhoods.manhattan.slice(0, 8).length} (top tier)`);
           console.log(`   🏆 Brooklyn: ${highPriorityNeighborhoods.brooklyn.slice(0, 4).length} (high value)`);
           console.log(`   🏆 Queens: ${highPriorityNeighborhoods.queens.slice(0, 2).length} (emerging)`);
           console.log(`   🏆 Bronx: ${highPriorityNeighborhoods.bronx.slice(0, 1).length} (affordable)`);
           
           return selectedNeighborhoods;
       }
   }

   logRailwaySummary(results) {
       console.log('\n📊 RAILWAY DEPLOYMENT SUMMARY');
       console.log('='.repeat(50));
       console.log(`✅ Status: ${results.success ? 'SUCCESS' : 'FAILED'}`);
       console.log(`🏠 Mode: Rent-Stabilized Only (Priority-based targeting)`);
       console.log(`⏱️ Duration: ${results.totalDuration} minutes`);
       
       if (results.rentStabilized) {
           const rs = results.rentStabilized;
           console.log(`📋 Total listings scanned: ${rs.totalListingsScanned}`);
           console.log(`⚖️ Rent-stabilized found: ${rs.rentStabilizedFound}`);
           console.log(`💰 Undervalued opportunities: ${rs.undervaluedStabilizedFound}`);
           
           if (rs.undervaluedStabilizedFound > 0) {
               console.log('\n🎯 USER VALUE:');
               console.log(`   💵 Best deals available: ${rs.undervaluedStabilizedFound} listings`);
               console.log(`   📊 Average savings potential: Check database for details`);
           }
       }
       
       console.log('\n💡 SYSTEM FEATURES:');
       console.log(`   📊 Sales scraper: DISABLED (focus on rent-stabilized)`);
       console.log(`   🏠 Rentals scraper: DISABLED (focus on rent-stabilized)`);
       console.log(`   ⚖️ Rent-stabilized finder: ACTIVE`);
       console.log(`   🎯 Priority-based targeting: ACTIVE`);
       console.log(`   ⚡ Smart caching: ACTIVE`);
       console.log(`   📁 DHCR verification: ACTIVE`);
   }

   delay(ms) {
       return new Promise(resolve => setTimeout(resolve, ms));
   }
}

// Railway Deployment Helper Functions (Updated)
class RailwayDeploymentHelper {
   static async checkEnvironmentVariables() {
       // Updated for rent-stabilized system
       const required = [
           'SUPABASE_URL', 
           'SUPABASE_ANON_KEY'
           // Note: RAPIDAPI_KEY may not be needed if not using external APIs
       ];
       
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
       console.log(`🏠 Target: Rent-Stabilized Apartments Only`);
       console.log(`🎯 Targeting: Priority-based neighborhood selection`);
       console.log(`⚡ Smart caching: ENABLED`);
       console.log(`📁 DHCR verification: ENABLED`);
       console.log(`📊 Expected duration: ${process.env.INITIAL_BULK_LOAD === 'true' ? '1-2 hours' : '20-40 minutes'}`);
       
       // Log environment-specific info
       if (process.env.TEST_NEIGHBORHOOD) {
           console.log(`🧪 Testing mode: ${process.env.TEST_NEIGHBORHOOD} only`);
       }
       if (process.env.FOCUS_BOROUGH) {
           console.log(`🎯 Borough focus: ${process.env.FOCUS_BOROUGH}`);
       }
       if (process.env.MANUAL_NEIGHBORHOODS) {
           console.log(`📝 Manual neighborhoods: ${process.env.MANUAL_NEIGHBORHOODS}`);
       }
       
       console.log('');
   }
   
   static setupGracefulShutdown(runner) {
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
           console.log('🏠 Railway Rent-Stabilized Finder');
           console.log('');
           console.log('Usage:');
           console.log('  node railway-sequential-runner.js           # Run rent-stabilized finder');
           console.log('  node railway-sequential-runner.js --help    # Show this help');
           console.log('');
           console.log('Environment Variables:');
           console.log('  INITIAL_BULK_LOAD=true          # Enable bulk load mode (all neighborhoods)');
           console.log('  INITIAL_BULK_LOAD=false         # Regular weekly mode (top neighborhoods)');
           console.log('  TEST_NEIGHBORHOOD=east-village  # Test single neighborhood');
           console.log('  FOCUS_BOROUGH=manhattan         # Focus on specific borough');
           console.log('  MANUAL_NEIGHBORHOODS=...        # Override with manual list');
           console.log('');
           console.log('Features:');
           console.log('  🎯 Priority-based targeting: Focuses on high-value neighborhoods');
           console.log('  ⚡ Smart caching: Reduces API calls through intelligent caching');
           console.log('  ⚖️ Legal analysis: 70%+ confidence rent-stabilization detection');
           console.log('  💰 Market analysis: Advanced bed/bath/amenity valuation');
           console.log('  📁 DHCR verification: Uses official DHCR building database');
           console.log('');
           console.log('Railway Deployment:');
           console.log('  1. Set environment variables in Railway Dashboard');
           console.log('  2. Deploy this file as your main entry point');
           console.log('  3. Add DHCR PDF files to data/dhcr/ directory');
           console.log('  4. System will automatically target priority neighborhoods');
           console.log('');
           console.log('Disabled Scripts:');
           console.log('  ❌ biweekly-streeteasy-sales.js (disabled)');
           console.log('  ❌ biweekly-streeteasy-rentals.js (disabled)');
           console.log('  ✅ rent-stabilized-undervalued-system.js (active)');
           return;
       }

       // Step 3: Run rent-stabilized analysis only
       const runner = new RailwayRentStabilizedRunner();
       RailwayDeploymentHelper.setupGracefulShutdown(runner);
       
       const results = await runner.runRentStabilizedAnalysis();
       
       console.log('\n✅ Railway deployment completed successfully!');
       console.log(`🎯 Undervalued rent-stabilized listings: ${results.rentStabilized?.undervaluedStabilizedFound || 0}`);
       
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
       console.error('   3. Check database schema is updated');
       console.error('   4. Review Railway logs for detailed errors');
       console.error('   5. Ensure DHCR PDF parsing is working');
       console.error('   6. Verify StreetEasy scraper integration');
       
       process.exit(1);
   }
}

// Export for testing
module.exports = { RailwayRentStabilizedRunner, RailwayDeploymentHelper };

// Run if executed directly (Railway entry point)
if (require.main === module) {
   main().catch(error => {
       console.error('💥 Railway rent-stabilized runner crashed:', error);
       process.exit(1);
   });
}
