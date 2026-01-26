import { drizzle } from 'drizzle-orm/libsql';
import { createClient } from '@libsql/client';
import * as schema from "@shared/schema";

const dbUrl = process.env.DATABASE_URL || "file:local.db";
let url = dbUrl;

// If it's a postgres URL from Replit, we convert it to a local file for the Windows/libsql request
// but in a real Windows env the user would provide a file: or libsql: URL.
if (url.startsWith("postgresql:")) {
  url = "file:local.db";
}

const client = createClient({
  url: url,
});

export const db = drizzle(client, { schema });
