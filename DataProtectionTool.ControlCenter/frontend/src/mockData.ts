import type { SavedConnection, TableInfo, QueryInfo } from "./components/ConnectionsPanel";
import type { PreviewData, SampleResult } from "./components/DataPreviewPanel";
import type { StatusEvent } from "./components/StatusBar";
import type { FlowItem } from "./components/FlowsPanel";

export function isDemoMode(): boolean {
  const segments = window.location.pathname.split("/");
  const agentsIdx = segments.indexOf("agents");
  return agentsIdx === -1 || agentsIdx + 1 >= segments.length;
}

const now = new Date();
function hoursAgo(h: number): string {
  return new Date(now.getTime() - h * 3600_000).toISOString();
}

// ---------------------------------------------------------------------------
// Connections
// ---------------------------------------------------------------------------

export const MOCK_CONNECTIONS: SavedConnection[] = [
  {
    rowKey: "conn-001",
    connectionType: "SqlServer",
    serverName: "prod-sql-east.database.windows.net",
    authentication: "SQL Login",
    databaseName: "CustomerDB",
    encrypt: "Yes",
    trustServerCertificate: false,
    createdAt: hoursAgo(48),
  },
  {
    rowKey: "conn-002",
    connectionType: "SqlServer",
    serverName: "staging-sql.corp.local",
    authentication: "Windows Authentication",
    databaseName: "HR_System",
    encrypt: "Yes",
    trustServerCertificate: true,
    createdAt: hoursAgo(24),
  },
  {
    rowKey: "conn-003",
    connectionType: "SqlServer",
    serverName: "dev-sql.corp.local",
    authentication: "SQL Login",
    databaseName: "Analytics",
    encrypt: "No",
    trustServerCertificate: true,
    createdAt: hoursAgo(2),
  },
];

// ---------------------------------------------------------------------------
// Tables per connection
// ---------------------------------------------------------------------------

export const MOCK_CONNECTION_TABLES: Record<string, TableInfo[]> = {
  "conn-001": [
    { schema: "dbo", name: "Customers", fileFormatId: "ff-cust-001" },
    { schema: "dbo", name: "Orders", fileFormatId: "ff-ord-001" },
    { schema: "dbo", name: "Payments", fileFormatId: "ff-pay-001" },
    { schema: "dbo", name: "Products" },
    { schema: "dbo", name: "OrderItems" },
    { schema: "dbo", name: "Invoices" },
    { schema: "dbo", name: "ShippingAddresses" },
    { schema: "dbo", name: "Returns" },
    { schema: "dbo", name: "Coupons" },
    { schema: "dbo", name: "Reviews" },
    { schema: "audit", name: "LoginHistory" },
    { schema: "audit", name: "ChangeLog" },
    { schema: "audit", name: "AccessRequests" },
    { schema: "config", name: "AppSettings" },
    { schema: "config", name: "FeatureFlags" },
  ],
  "conn-002": [
    { schema: "hr", name: "Employees", fileFormatId: "ff-emp-001" },
    { schema: "hr", name: "Departments" },
    { schema: "hr", name: "Salaries", fileFormatId: "ff-sal-001" },
    { schema: "hr", name: "Benefits" },
    { schema: "hr", name: "PerformanceReviews" },
    { schema: "hr", name: "TimeOff" },
    { schema: "hr", name: "Positions" },
    { schema: "hr", name: "Candidates" },
    { schema: "hr", name: "Interviews" },
    { schema: "hr", name: "Onboarding" },
    { schema: "payroll", name: "PayStubs" },
    { schema: "payroll", name: "TaxWithholdings" },
    { schema: "payroll", name: "DirectDeposits" },
    { schema: "compliance", name: "TrainingRecords" },
    { schema: "compliance", name: "Certifications" },
  ],
  "conn-003": [
    { schema: "dbo", name: "PageViews" },
    { schema: "dbo", name: "UserSessions" },
    { schema: "dbo", name: "ClickEvents" },
    { schema: "dbo", name: "SearchQueries" },
    { schema: "dbo", name: "ConversionFunnels" },
    { schema: "dbo", name: "ABTestResults" },
    { schema: "dbo", name: "ErrorLogs" },
    { schema: "dbo", name: "PerformanceMetrics" },
    { schema: "staging", name: "RawImports" },
    { schema: "staging", name: "TransformQueue" },
  ],
};

// ---------------------------------------------------------------------------
// Table columns (for expand/collapse in sidebar)
// ---------------------------------------------------------------------------

export const MOCK_TABLE_COLUMNS: Record<string, { name: string; type: string }[]> = {
  "conn-001:dbo:Customers": [
    { name: "CustomerID", type: "int" },
    { name: "FirstName", type: "nvarchar" },
    { name: "LastName", type: "nvarchar" },
    { name: "Email", type: "nvarchar" },
    { name: "Phone", type: "nvarchar" },
    { name: "SSN", type: "nvarchar" },
    { name: "DateOfBirth", type: "date" },
    { name: "Address", type: "nvarchar" },
    { name: "City", type: "nvarchar" },
    { name: "State", type: "nvarchar" },
    { name: "ZipCode", type: "nvarchar" },
  ],
  "conn-001:dbo:Orders": [
    { name: "OrderID", type: "int" },
    { name: "CustomerID", type: "int" },
    { name: "OrderDate", type: "datetime" },
    { name: "TotalAmount", type: "decimal" },
    { name: "Status", type: "nvarchar" },
  ],
  "conn-001:dbo:Payments": [
    { name: "PaymentID", type: "int" },
    { name: "OrderID", type: "int" },
    { name: "Amount", type: "decimal" },
    { name: "PaymentMethod", type: "nvarchar" },
    { name: "PaymentDate", type: "datetime" },
  ],
  "conn-002:hr:Employees": [
    { name: "EmployeeID", type: "int" },
    { name: "FullName", type: "nvarchar" },
    { name: "Email", type: "nvarchar" },
    { name: "Department", type: "nvarchar" },
    { name: "Salary", type: "decimal" },
    { name: "HireDate", type: "date" },
    { name: "ManagerID", type: "int" },
  ],
  "conn-002:hr:Departments": [
    { name: "DepartmentID", type: "int" },
    { name: "DepartmentName", type: "nvarchar" },
    { name: "ManagerID", type: "int" },
    { name: "Budget", type: "decimal" },
  ],
  "conn-002:hr:Salaries": [
    { name: "SalaryID", type: "int" },
    { name: "EmployeeID", type: "int" },
    { name: "BasePay", type: "decimal" },
    { name: "Bonus", type: "decimal" },
    { name: "EffectiveDate", type: "date" },
  ],
};

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

export const MOCK_CONNECTION_QUERIES: Record<string, QueryInfo[]> = {
  "conn-001": [
    {
      rowKey: "query-001",
      connectionRowKey: "conn-001",
      queryText: "SELECT TOP 100 * FROM dbo.Customers WHERE Country = 'US'",
      createdAt: hoursAgo(12),
    },
  ],
  "conn-002": [],
  "conn-003": [],
};

// ---------------------------------------------------------------------------
// Sample preview data (PII-rich for demo)
// ---------------------------------------------------------------------------

const CUSTOMERS_PREVIEW: PreviewData = {
  headers: [
    "CustomerID", "FirstName", "LastName", "Email",
    "Phone", "SSN", "DateOfBirth", "Address",
    "City", "State", "ZipCode",
  ],
  columnTypes: [
    "int", "nvarchar", "nvarchar", "nvarchar",
    "nvarchar", "nvarchar", "date", "nvarchar",
    "nvarchar", "nvarchar", "nvarchar",
  ],
  rows: [
    ["1001", "Alice", "Johnson", "alice.johnson@email.com", "(555) 123-4567", "123-45-6789", "1985-03-15", "742 Evergreen Terrace", "Springfield", "IL", "62704"],
    ["1002", "Bob", "Williams", "bob.w@company.org", "(555) 234-5678", "234-56-7890", "1990-07-22", "1600 Pennsylvania Ave", "Washington", "DC", "20500"],
    ["1003", "Carol", "Davis", "carol.davis@example.net", "(555) 345-6789", "345-67-8901", "1978-11-08", "221B Baker Street", "New York", "NY", "10001"],
    ["1004", "David", "Martinez", "d.martinez@mail.com", "(555) 456-7890", "456-78-9012", "1995-01-30", "350 Fifth Avenue", "New York", "NY", "10118"],
    ["1005", "Eva", "Chen", "eva.chen@tech.io", "(555) 567-8901", "567-89-0123", "1988-09-12", "1 Infinite Loop", "Cupertino", "CA", "95014"],
    ["1006", "Frank", "O'Brien", "frank.ob@corp.com", "(555) 678-9012", "678-90-1234", "1972-04-25", "456 Oak Drive", "Portland", "OR", "97201"],
    ["1007", "Grace", "Kim", "grace.kim@web.co", "(555) 789-0123", "789-01-2345", "2000-12-01", "789 Pine Lane", "Seattle", "WA", "98101"],
    ["1008", "Henry", "Patel", "h.patel@startup.dev", "(555) 890-1234", "890-12-3456", "1983-06-18", "12 Main Street", "Boston", "MA", "02101"],
  ],
};

const CUSTOMERS_MASKED_PREVIEW: PreviewData = {
  headers: CUSTOMERS_PREVIEW.headers,
  columnTypes: CUSTOMERS_PREVIEW.columnTypes,
  rows: [
    ["1001", "A****", "J******", "a****.j******@****.com", "(***) ***-4567", "***-**-6789", "1985-01-01", "*** Evergreen *******", "Springfield", "IL", "627**"],
    ["1002", "B**", "W*******", "b**.w@*******.org", "(***) ***-5678", "***-**-7890", "1990-01-01", "**** Pennsylvania ***", "Washington", "DC", "205**"],
    ["1003", "C****", "D****", "c****.d****@*******.net", "(***) ***-6789", "***-**-8901", "1978-01-01", "**** Baker ******", "New York", "NY", "100**"],
    ["1004", "D****", "M*******", "d.m*******@****.com", "(***) ***-7890", "***-**-9012", "1995-01-01", "*** Fifth ******", "New York", "NY", "101**"],
    ["1005", "E**", "C***", "e**.c***@****.io", "(***) ***-8901", "***-**-0123", "1988-01-01", "* Infinite ****", "Cupertino", "CA", "950**"],
    ["1006", "F****", "O'B****", "f****.ob@****.com", "(***) ***-9012", "***-**-1234", "1972-01-01", "*** Oak *****", "Portland", "OR", "972**"],
    ["1007", "G****", "K**", "g****.k**@***.co", "(***) ***-0123", "***-**-2345", "2000-01-01", "*** Pine ****", "Seattle", "WA", "981**"],
    ["1008", "H****", "P****", "h.p****@*******.dev", "(***) ***-1234", "***-**-3456", "1983-01-01", "** Main ******", "Boston", "MA", "021**"],
  ],
};

const EMPLOYEES_PREVIEW: PreviewData = {
  headers: ["EmployeeID", "FullName", "Email", "Department", "Salary", "HireDate", "ManagerID"],
  columnTypes: ["int", "nvarchar", "nvarchar", "nvarchar", "decimal", "date", "int"],
  rows: [
    ["201", "Sarah Thompson", "s.thompson@corp.local", "Engineering", "125000.00", "2018-03-01", "200"],
    ["202", "James Lee", "j.lee@corp.local", "Engineering", "118000.00", "2019-06-15", "201"],
    ["203", "Maria Garcia", "m.garcia@corp.local", "Marketing", "95000.00", "2020-01-10", "210"],
    ["204", "Robert Wilson", "r.wilson@corp.local", "Finance", "105000.00", "2017-09-20", "220"],
    ["205", "Lisa Anderson", "l.anderson@corp.local", "Engineering", "132000.00", "2016-11-05", "201"],
  ],
};

const EMPLOYEES_MASKED_PREVIEW: PreviewData = {
  headers: EMPLOYEES_PREVIEW.headers,
  columnTypes: EMPLOYEES_PREVIEW.columnTypes,
  rows: [
    ["201", "S**** T*******", "s.t*******@****.local", "Engineering", "125000.00", "2018-01-01", "200"],
    ["202", "J**** L**", "j.l**@****.local", "Engineering", "118000.00", "2019-01-01", "201"],
    ["203", "M**** G*****", "m.g*****@****.local", "Marketing", "95000.00", "2020-01-01", "210"],
    ["204", "R***** W*****", "r.w*****@****.local", "Finance", "105000.00", "2017-01-01", "220"],
    ["205", "L*** A*******", "l.a*******@****.local", "Engineering", "132000.00", "2016-01-01", "201"],
  ],
};

export const MOCK_SAMPLE_DATA: Record<string, SampleResult[]> = {
  "conn-001:dbo:Customers": [
    { label: "Sample 1", data: CUSTOMERS_PREVIEW, blobFilenames: ["mock-cust-sample.parquet"] },
  ],
  "conn-002:hr:Employees": [
    { label: "Sample 1", data: EMPLOYEES_PREVIEW, blobFilenames: ["mock-emp-sample.parquet"] },
  ],
};

export const MOCK_DRY_RUN_DATA: Record<string, PreviewData> = {
  "conn-001:dbo:Customers": CUSTOMERS_MASKED_PREVIEW,
  "conn-002:hr:Employees": EMPLOYEES_MASKED_PREVIEW,
};

// ---------------------------------------------------------------------------
// Column rules
// ---------------------------------------------------------------------------

export const MOCK_COLUMN_RULES: Record<string, unknown>[] = [
  { fileFieldMetadataId: "ffm-001", fieldName: "FirstName", algorithmName: "PartialMask", domainName: "Name", isMasked: true, ordinalPosition: 1 },
  { fileFieldMetadataId: "ffm-002", fieldName: "LastName", algorithmName: "PartialMask", domainName: "Name", isMasked: true, ordinalPosition: 2 },
  { fileFieldMetadataId: "ffm-003", fieldName: "Email", algorithmName: "EmailMask", domainName: "Email", isMasked: true, ordinalPosition: 3 },
  { fileFieldMetadataId: "ffm-004", fieldName: "Phone", algorithmName: "PhoneMask", domainName: "Phone", isMasked: true, ordinalPosition: 4 },
  { fileFieldMetadataId: "ffm-005", fieldName: "SSN", algorithmName: "SSNMask", domainName: "SSN", isMasked: true, ordinalPosition: 5 },
  { fileFieldMetadataId: "ffm-006", fieldName: "DateOfBirth", algorithmName: "DateShift", domainName: "Date", isMasked: true, ordinalPosition: 6 },
  { fileFieldMetadataId: "ffm-007", fieldName: "Address", algorithmName: "AddressMask", domainName: "Address", isMasked: true, ordinalPosition: 7 },
  { fileFieldMetadataId: "ffm-008", fieldName: "ZipCode", algorithmName: "ZipMask", domainName: "Address", isMasked: true, ordinalPosition: 8 },
  { fileFieldMetadataId: "ffm-009", fieldName: "CustomerID", algorithmName: "", domainName: "", isMasked: false, ordinalPosition: 0 },
  { fileFieldMetadataId: "ffm-010", fieldName: "City", algorithmName: "", domainName: "", isMasked: false, ordinalPosition: 9 },
  { fileFieldMetadataId: "ffm-011", fieldName: "State", algorithmName: "", domainName: "", isMasked: false, ordinalPosition: 10 },
];

export const MOCK_COLUMN_RULE_ALGORITHMS: Record<string, unknown>[] = [
  { algorithmName: "PartialMask", maskType: "STRING", description: "Partially masks string values, preserving first character" },
  { algorithmName: "EmailMask", maskType: "STRING", description: "Masks email local part while preserving domain structure" },
  { algorithmName: "PhoneMask", maskType: "STRING", description: "Masks phone digits, preserving last 4" },
  { algorithmName: "SSNMask", maskType: "STRING", description: "Masks SSN, preserving last 4 digits" },
  { algorithmName: "DateShift", maskType: "LOCAL_DATE_TIME", description: "Shifts dates by a consistent random offset" },
  { algorithmName: "AddressMask", maskType: "STRING", description: "Masks street address details" },
  { algorithmName: "ZipMask", maskType: "STRING", description: "Masks last 2 digits of zip code" },
  { algorithmName: "FullRedact", maskType: "STRING", description: "Fully redacts the value" },
  { algorithmName: "NumericRound", maskType: "BIG_DECIMAL", description: "Rounds numeric values to nearest bucket" },
];

export const MOCK_COLUMN_RULE_DOMAINS: Record<string, unknown>[] = [
  { domainName: "Name", description: "Personal names" },
  { domainName: "Email", description: "Email addresses" },
  { domainName: "Phone", description: "Phone numbers" },
  { domainName: "SSN", description: "Social Security Numbers" },
  { domainName: "Date", description: "Date values" },
  { domainName: "Address", description: "Physical addresses" },
  { domainName: "Financial", description: "Financial data" },
];

export const MOCK_COLUMN_RULE_FRAMEWORKS: Record<string, unknown>[] = [
  { frameworkName: "GDPR", description: "EU General Data Protection Regulation" },
  { frameworkName: "CCPA", description: "California Consumer Privacy Act" },
  { frameworkName: "HIPAA", description: "Health Insurance Portability and Accountability Act" },
];

// ---------------------------------------------------------------------------
// Engine metadata (global algorithms / domains / frameworks)
// ---------------------------------------------------------------------------

export const MOCK_ALL_ALGORITHMS = MOCK_COLUMN_RULE_ALGORITHMS;
export const MOCK_ALL_DOMAINS = MOCK_COLUMN_RULE_DOMAINS;
export const MOCK_ALL_FRAMEWORKS = MOCK_COLUMN_RULE_FRAMEWORKS;

// ---------------------------------------------------------------------------
// Status events
// ---------------------------------------------------------------------------

export const MOCK_STATUS_EVENTS: StatusEvent[] = [
  {
    timestamp: hoursAgo(48),
    type: "connection",
    summary: "Connection saved: prod-sql-east.database.windows.net / CustomerDB",
    detail: "",
    steps: [
      { timestamp: hoursAgo(48), message: "Validating connection string...", status: "done" },
      { timestamp: hoursAgo(48), message: "Connection validated successfully", status: "done" },
      { timestamp: hoursAgo(48), message: "Connection saved to storage", status: "done" },
    ],
  },
  {
    timestamp: hoursAgo(24),
    type: "connection",
    summary: "Connection saved: staging-sql.corp.local / HR_System",
    detail: "",
    steps: [
      { timestamp: hoursAgo(24), message: "Validating connection string...", status: "done" },
      { timestamp: hoursAgo(24), message: "Connection validated successfully", status: "done" },
      { timestamp: hoursAgo(24), message: "Connection saved to storage", status: "done" },
    ],
  },
  {
    timestamp: hoursAgo(6),
    type: "list_tables",
    summary: "Tables listed: CustomerDB (4 tables found)",
    detail: "",
    steps: [
      { timestamp: hoursAgo(6), message: "Connecting to prod-sql-east.database.windows.net...", status: "done" },
      { timestamp: hoursAgo(6), message: "Querying table metadata...", status: "done" },
      { timestamp: hoursAgo(6), message: "Found 4 tables", status: "done" },
    ],
  },
  {
    timestamp: hoursAgo(1),
    type: "dp_preview",
    summary: "DP preview completed: dbo.Customers",
    detail: "",
    steps: [
      { timestamp: hoursAgo(1), message: "Sampling 8 rows from dbo.Customers...", status: "done" },
      { timestamp: hoursAgo(1), message: "Applying masking rules (8 columns)...", status: "done" },
      { timestamp: hoursAgo(1), message: "Generating masked preview...", status: "done" },
      { timestamp: hoursAgo(1), message: "DP preview complete", status: "done" },
    ],
  },
];

// ---------------------------------------------------------------------------
// Flows
// ---------------------------------------------------------------------------

export const MOCK_FLOWS: FlowItem[] = [
  {
    rowKey: "flow-001",
    sourceJson: JSON.stringify({
      connectionRowKey: "conn-001",
      serverName: "prod-sql-east.database.windows.net",
      databaseName: "CustomerDB",
      schema: "dbo",
      tableName: "Customers",
    }),
    destJson: JSON.stringify({
      connectionRowKey: "conn-003",
      serverName: "dev-sql.corp.local",
      databaseName: "Analytics",
      schema: "dbo",
      tableName: "Customers",
    }),
    createdAt: hoursAgo(3),
  },
  {
    rowKey: "flow-003",
    sourceJson: JSON.stringify({
      connectionRowKey: "conn-001",
      serverName: "prod-sql-east.database.windows.net",
      databaseName: "CustomerDB",
      schema: "dbo",
      tableName: "Orders",
    }),
    destJson: JSON.stringify({
      connectionRowKey: "conn-003",
      serverName: "dev-sql.corp.local",
      databaseName: "Analytics",
      schema: "dbo",
      tableName: "Orders",
    }),
    createdAt: hoursAgo(2),
  },
  {
    rowKey: "flow-004",
    sourceJson: JSON.stringify({
      connectionRowKey: "conn-001",
      serverName: "prod-sql-east.database.windows.net",
      databaseName: "CustomerDB",
      schema: "dbo",
      tableName: "Payments",
    }),
    destJson: JSON.stringify({
      connectionRowKey: "conn-003",
      serverName: "dev-sql.corp.local",
      databaseName: "Analytics",
      schema: "dbo",
      tableName: "Payments",
    }),
    createdAt: hoursAgo(2),
  },
  {
    rowKey: "flow-002",
    sourceJson: JSON.stringify({
      connectionRowKey: "conn-002",
      serverName: "staging-sql.corp.local",
      databaseName: "HR_System",
      schema: "hr",
      tableName: "Employees",
    }),
    destJson: JSON.stringify({
      connectionRowKey: "conn-003",
      serverName: "dev-sql.corp.local",
      databaseName: "Analytics",
      schema: "hr",
      tableName: "Employees",
    }),
    createdAt: hoursAgo(1),
  },
];
