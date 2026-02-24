# cheesecloth
Stock screener utilizing the XBRL SEC API.
  
## Roadmap
### Core Functionality
- data ingestion through API
- clean and process data
- CLI interface for ease of use

### Next Up
- Add comments for why the code is designed like how it is
- add more views to cli
    - map out EDGAR API qname taxonomy for EPS, gross income, net income, cash flow, margins, debt, etc
- add abstractions for hardcoded tuple parsing  
- logging control when scraping from cli
- add non-cli script to scrape data
- quarterly view alongside annual