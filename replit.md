# FlowAuto - Visual Workflow Automation Platform

## Overview

FlowAuto is a visual workflow automation platform similar to n8n or Zapier. Users can create, edit, and execute automation workflows using a drag-and-drop canvas interface. The application features AI-powered workflow generation, multiple node types for different integrations (HTTP requests, databases, S3, Airflow), and execution logging.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Frontend Architecture
- **Framework**: React 18 with TypeScript
- **Routing**: Wouter (lightweight alternative to React Router)
- **State Management**: TanStack Query for server state caching and synchronization
- **UI Components**: shadcn/ui component library built on Radix UI primitives
- **Styling**: Tailwind CSS with custom theme configuration and CSS variables
- **Workflow Canvas**: ReactFlow for visual node-based editor with custom node types
- **Build Tool**: Vite with path aliases (@/, @shared/, @assets/)

### Backend Architecture
- **Framework**: Express.js with TypeScript running on Node.js
- **API Design**: RESTful endpoints under `/api/` prefix with Zod schema validation
- **Database ORM**: Drizzle ORM with PostgreSQL
- **Session Storage**: PostgreSQL-backed sessions via connect-pg-simple
- **Build Process**: esbuild for server bundling, Vite for client

### Data Storage
- **Database**: PostgreSQL (required via DATABASE_URL environment variable)
- **Schema Location**: `shared/schema.ts` contains all table definitions
- **Tables**:
  - `workflows`: Stores workflow definitions with nodes/edges as JSONB
  - `executions`: Tracks workflow execution history and logs
  - `conversations/messages`: Chat storage for AI integrations

### Code Organization
- `client/`: React frontend application
- `server/`: Express backend with routes, storage layer, and integrations
- `shared/`: Shared types, schemas, and route definitions used by both client and server
- `server/replit_integrations/`: Pre-built modules for audio, chat, image, and batch processing

### Key Design Patterns
- **Storage Interface**: Abstract `IStorage` interface in `server/storage.ts` enables swapping implementations
- **Route Contracts**: Shared route definitions in `shared/routes.ts` ensure type safety between client and server
- **Custom React Flow Nodes**: Node components in `client/src/components/CustomNodes.tsx` for different workflow step types

## External Dependencies

### AI Services
- **OpenAI API**: Used for AI workflow generation and chat features
- **Environment Variables**: 
  - `AI_INTEGRATIONS_OPENAI_API_KEY`
  - `AI_INTEGRATIONS_OPENAI_BASE_URL`

### Cloud Integrations
- **AWS S3**: SDK included for S3 node functionality (`@aws-sdk/client-s3`)
- **Database Connections**: mysql2 and mssql packages for SQL database nodes

### Database
- **PostgreSQL**: Primary database via `DATABASE_URL` environment variable
- **Drizzle Kit**: Database migration tool (`npm run db:push`)

### Development
- **Replit Plugins**: Runtime error overlay, cartographer, and dev banner for Replit environment
- **ffmpeg**: Required system dependency for audio format conversion in voice features