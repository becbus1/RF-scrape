// comprehensive-nyc-neighborhoods.js
// Complete list of NYC neighborhoods supported by StreetEasy API

const NYC_NEIGHBORHOODS = {
    // MANHATTAN (30+ neighborhoods)
    manhattan: [
        'West Village', 'East Village', 'Greenwich Village', 'SoHo', 'NoLita',
        'Tribeca', 'Financial District', 'Battery Park City', 'Lower East Side',
        'Chinatown', 'Little Italy', 'Flatiron District', 'Gramercy',
        'Murray Hill', 'Kips Bay', 'Union Square', 'Chelsea', 'Midtown',
        'Hells Kitchen', 'Upper East Side', 'Upper West Side', 'Lincoln Square',
        'Carnegie Hill', 'Yorkville', 'Lenox Hill', 'Sutton Place',
        'Turtle Bay', 'Midtown East', 'Midtown West', 'Morningside Heights',
        'Harlem', 'East Harlem', 'Hamilton Heights', 'Washington Heights',
        'Inwood', 'Hudson Square'
    ],

    // BROOKLYN (50+ neighborhoods)
    brooklyn: [
        'Brooklyn Heights', 'DUMBO', 'Downtown Brooklyn', 'Fort Greene',
        'Boerum Hill', 'Cobble Hill', 'Carroll Gardens', 'Red Hook',
        'Park Slope', 'Prospect Heights', 'Crown Heights', 'Bedford-Stuyvesant',
        'Williamsburg', 'Greenpoint', 'East Williamsburg', 'Bushwick',
        'Ridgewood', 'Bed-Stuy', 'Clinton Hill', 'Prospect Lefferts Gardens',
        'Windsor Terrace', 'Kensington', 'Ditmas Park', 'Flatbush',
        'East Flatbush', 'Flatlands', 'Midwood', 'Sheepshead Bay',
        'Brighton Beach', 'Coney Island', 'Bensonhurst', 'Bay Ridge',
        'Sunset Park', 'Greenwood', 'Borough Park', 'Dyker Heights',
        'Bath Beach', 'Gravesend', 'Canarsie', 'East New York',
        'Brownsville', 'Ocean Hill', 'Stuyvesant Heights', 'Crown Heights North',
        'Crown Heights South', 'Gowanus', 'Columbia Street Waterfront',
        'Vinegar Hill', 'Navy Yard', 'New Lots', 'City Line'
    ],

    // QUEENS (30+ neighborhoods)
    queens: [
        'Long Island City', 'Hunters Point', 'Astoria', 'Sunnyside',
        'Woodside', 'Jackson Heights', 'Elmhurst', 'Corona',
        'Flushing', 'College Point', 'Whitestone', 'Ridgewood',
        'Maspeth', 'Middle Village', 'Glendale', 'Forest Hills',
        'Rego Park', 'Kew Gardens', 'Richmond Hill', 'South Ozone Park',
        'Howard Beach', 'Ozone Park', 'Woodhaven', 'Jamaica',
        'St. Albans', 'Hollis', 'Queens Village', 'Cambria Heights',
        'Laurelton', 'Rosedale', 'Far Rockaway', 'Rockaway Beach',
        'Arverne', 'Breezy Point', 'Belle Harbor', 'Neponsit',
        'Broad Channel', 'Little Neck', 'Douglaston', 'Bayside',
        'Oakland Gardens', 'Fresh Meadows', 'Briarwood'
    ],

    // BRONX (25+ neighborhoods)
    bronx: [
        'Mott Haven', 'South Bronx', 'Port Morris', 'Melrose',
        'Morrisania', 'Hunts Point', 'Longwood', 'Concourse',
        'High Bridge', 'Mount Eden', 'Morris Heights', 'University Heights',
        'Fordham', 'Belmont', 'Tremont', 'East Tremont', 'West Farms',
        'Bronx Park', 'Norwood', 'Bedford Park', 'Kingsbridge',
        'Riverdale', 'Spuyten Duyvil', 'Marble Hill', 'Fieldston',
        'Van Cortlandt Village', 'Woodlawn', 'Wakefield', 'Eastchester',
        'Baychester', 'Co-op City', 'Castle Hill', 'Parkchester',
        'Soundview', 'Clason Point', 'Throggs Neck', 'Country Club',
        'Pelham Bay', 'Pelham Gardens', 'Westchester Square', 'Schuylerville',
        'Edenwald', 'Bronxwood', 'Woodstock', 'Westchester Village'
    ],

    // STATEN ISLAND (15+ neighborhoods)
    staten_island: [
        'St. George', 'Stapleton', 'Clifton', 'Rosebank', 'South Beach',
        'New Brighton', 'West Brighton', 'Port Richmond', 'Mariners Harbor',
        'Arlington', 'Graniteville', 'Bulls Head', 'Bloomfield',
        'Meiers Corners', 'Willowbrook', 'Todt Hill', 'Dongan Hills',
        'Midland Beach', 'New Dorp', 'Oakwood', 'Bay Terrace',
        'Richmondtown', 'Arden Heights', 'Annadale', 'Huguenot',
        'Eltingville', 'Great Kills', 'Bay Ridge', 'Tottenville',
        'Charleston', 'Rossville', 'Woodrow', 'Pleasant Plains'
    ]
};

// Flatten all neighborhoods into a single array
const ALL_NYC_NEIGHBORHOODS = [
    ...NYC_NEIGHBORHOODS.manhattan,
    ...NYC_NEIGHBORHOODS.brooklyn,
    ...NYC_NEIGHBORHOODS.queens,
    ...NYC_NEIGHBORHOODS.bronx,
    ...NYC_NEIGHBORHOODS.staten_island
];

// High-priority neighborhoods (most active markets based on StreetEasy data)
const HIGH_PRIORITY_NEIGHBORHOODS = [
    // Manhattan hotspots
    'West Village', 'East Village', 'SoHo', 'Tribeca', 'Chelsea',
    'Upper East Side', 'Upper West Side', 'Financial District', 'Lower East Side',
    
    // Brooklyn hotspots  
    'Park Slope', 'Williamsburg', 'DUMBO', 'Brooklyn Heights', 'Fort Greene',
    'Prospect Heights', 'Crown Heights', 'Bedford-Stuyvesant', 'Greenpoint',
    'Red Hook', 'Carroll Gardens', 'Bushwick', 'Sunset Park', 'Windsor Terrace',
    
    // Queens hotspots
    'Long Island City', 'Hunters Point', 'Astoria', 'Sunnyside', 'Ridgewood',
    'Woodside', 'Jackson Heights', 'Forest Hills', 'Kew Gardens',
    
    // Bronx opportunities
    'Mott Haven', 'South Bronx', 'Concourse', 'Fordham', 'Riverdale',
    
    // Staten Island
    'St. George', 'Stapleton', 'New Brighton'
];

// Export for use in other modules
module.exports = {
    ALL_NYC_NEIGHBORHOODS,
    NYC_NEIGHBORHOODS,
    HIGH_PRIORITY_NEIGHBORHOODS,
    
    // Helper functions
    getNeighborhoodsByBorough: (borough) => {
        return NYC_NEIGHBORHOODS[borough.toLowerCase()] || [];
    },
    
    getTotalNeighborhoodCount: () => {
        return ALL_NYC_NEIGHBORHOODS.length;
    },
    
    // Get neighborhoods in priority order for API efficiency
    getPriorityNeighborhoods: (limit = 50) => {
        return HIGH_PRIORITY_NEIGHBORHOODS.slice(0, limit);
    }
};
