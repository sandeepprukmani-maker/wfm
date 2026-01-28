import { useState } from "react";
import { useCredentials, useCreateCredential, useDeleteCredential } from "@/hooks/use-credentials";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import { 
  Key, 
  Plus, 
  Trash2, 
  Database, 
  Cloud, 
  Loader2,
  Lock,
  ExternalLink
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { format } from "date-fns";
import { cn } from "@/lib/utils";

export default function Credentials() {
  const [isOpen, setIsOpen] = useState(false);
  const { data: credentials, isLoading } = useCredentials();
  const { mutateAsync: createCredential } = useCreateCredential();
  const { mutate: deleteCredential } = useDeleteCredential();

  const formSchema = z.object({
    name: z.string().min(1, "Name is required"),
    type: z.enum(["airflow", "mssql", "postgres", "s3", "sftp"]),
    baseUrl: z.string().optional(),
    host: z.string().optional(),
    port: z.string().optional(),
    database: z.string().optional(),
    username: z.string().optional(),
    password: z.string().optional(),
    accessKey: z.string().optional(),
    secretKey: z.string().optional(),
    region: z.string().optional(),
    bucket: z.string().optional(),
  });

  const form = useForm<z.infer<typeof formSchema>>({
    resolver: zodResolver(formSchema),
    defaultValues: { type: "airflow", port: "1433" }
  });

  const credentialType = form.watch("type");

  const onSubmit = async (values: z.infer<typeof formSchema>) => {
    let data = {};
    if (values.type === 'airflow') {
      data = { baseUrl: values.baseUrl, username: values.username, password: values.password };
    } else if (values.type === 'mssql' || values.type === 'postgres') {
      data = { host: values.host, port: values.port || (values.type === 'mssql' ? "1433" : "5432"), database: values.database, username: values.username, password: values.password };
    } else if (values.type === 's3') {
      data = { accessKey: values.accessKey, secretKey: values.secretKey, region: values.region || 'us-east-1', bucket: values.bucket };
    } else if (values.type === 'sftp') {
      data = { host: values.host, port: values.port || "22", username: values.username, password: values.password };
    }

    await createCredential({
      name: values.name,
      type: values.type,
      data
    });
    setIsOpen(false);
    form.reset();
  };

  return (
    <div className="p-8 space-y-8 max-w-5xl mx-auto">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Credentials</h1>
          <p className="text-muted-foreground">Securely manage your Airflow and SQL Server connections</p>
        </div>

        <Dialog open={isOpen} onOpenChange={setIsOpen}>
          <DialogTrigger asChild>
            <Button className="rounded-xl">
              <Plus className="w-4 h-4 mr-2" />
              Add Credential
            </Button>
          </DialogTrigger>
          <DialogContent className="sm:max-w-[425px]">
            <DialogHeader>
              <DialogTitle>New Connection</DialogTitle>
            </DialogHeader>
            <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-4 pt-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">Name</label>
                <Input {...form.register("name")} placeholder="e.g. Production Airflow" required />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Type</label>
                <Select 
                  onValueChange={(val) => form.setValue("type", val as any)} 
                  defaultValue={credentialType}
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="airflow">Apache Airflow</SelectItem>
                    <SelectItem value="mssql">MS SQL Server</SelectItem>
                    <SelectItem value="postgres">PostgreSQL</SelectItem>
                    <SelectItem value="s3">AWS S3</SelectItem>
                    <SelectItem value="sftp">SFTP Server</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              {credentialType === "airflow" && (
                <>
                  <Input {...form.register("baseUrl")} placeholder="Base URL (e.g. http://airflow:8080)" required />
                  <Input {...form.register("username")} placeholder="Username" required />
                  <Input type="password" {...form.register("password")} placeholder="Password / API Key" required />
                </>
              )}

              {(credentialType === "mssql" || credentialType === "postgres") && (
                <>
                  <Input {...form.register("host")} placeholder="Server Address (Host)" required />
                  <Input {...form.register("port")} placeholder={credentialType === "mssql" ? "Port (Default: 1433)" : "Port (Default: 5432)"} />
                  <Input {...form.register("database")} placeholder="Database Name" required />
                  <Input {...form.register("username")} placeholder="Username" required />
                  <Input type="password" {...form.register("password")} placeholder="Password" required />
                </>
              )}

              {credentialType === "s3" && (
                <>
                  <Input {...form.register("accessKey")} placeholder="AWS Access Key" required />
                  <Input type="password" {...form.register("secretKey")} placeholder="AWS Secret Key" required />
                  <Input {...form.register("region")} placeholder="Region (Default: us-east-1)" />
                  <Input {...form.register("bucket")} placeholder="Default Bucket (Optional)" />
                </>
              )}

              {credentialType === "sftp" && (
                <>
                  <Input {...form.register("host")} placeholder="SFTP Host" required />
                  <Input {...form.register("port")} placeholder="Port (Default: 22)" />
                  <Input {...form.register("username")} placeholder="Username" required />
                  <Input type="password" {...form.register("password")} placeholder="Password" required />
                </>
              )}

              <Button type="submit" className="w-full rounded-xl" disabled={form.formState.isSubmitting}>
                {form.formState.isSubmitting && <Loader2 className="w-4 h-4 mr-2 animate-spin" />}
                Save Credential
              </Button>
            </form>
          </DialogContent>
        </Dialog>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {isLoading ? (
          <div className="col-span-2 flex justify-center py-12">
            <Loader2 className="w-8 h-8 animate-spin text-primary" />
          </div>
        ) : credentials?.length === 0 ? (
          <Card className="col-span-2 border-dashed bg-muted/20">
            <CardContent className="flex flex-col items-center justify-center py-12 text-center">
              <Key className="w-12 h-12 text-muted-foreground/30 mb-4" />
              <CardTitle className="text-muted-foreground">No credentials found</CardTitle>
              <CardDescription>Add a connection to start running real workflows</CardDescription>
            </CardContent>
          </Card>
        ) : (
          credentials?.map((cred) => (
            <Card key={cred.id} className="group hover:border-primary/50 transition-colors">
              <CardHeader className="flex flex-row items-center justify-between space-y-0">
                <div className="flex items-center gap-3">
                  <div className={cn(
                    "w-10 h-10 rounded-xl flex items-center justify-center",
                    cred.type === 'airflow' ? "bg-blue-500/20 text-blue-400" : 
                    cred.type === 's3' ? "bg-orange-500/20 text-orange-400" :
                    cred.type === 'sftp' ? "bg-purple-500/20 text-purple-400" :
                    "bg-green-500/20 text-green-400"
                  )}>
                    {cred.type === 'airflow' ? <Cloud className="w-5 h-5" /> : <Database className="w-5 h-5" />}
                  </div>
                  <div>
                    <CardTitle className="text-lg">{cred.name}</CardTitle>
                    <CardDescription className="capitalize">
                      {cred.type === 'mssql' ? 'SQL Server' : 
                       cred.type === 'airflow' ? 'Airflow' :
                       cred.type === 'postgres' ? 'PostgreSQL' :
                       cred.type === 's3' ? 'AWS S3' :
                       cred.type === 'sftp' ? 'SFTP' : cred.type}
                    </CardDescription>
                  </div>
                </div>
                <Button 
                  variant="ghost" 
                  size="icon" 
                  className="opacity-0 group-hover:opacity-100 transition-opacity text-destructive hover:bg-destructive/10"
                  onClick={() => { if(confirm('Delete credential?')) deleteCredential(cred.id) }}
                >
                  <Trash2 className="w-4 h-4" />
                </Button>
              </CardHeader>
              <CardContent>
                <div className="space-y-3">
                  <div className="flex items-center gap-2 text-xs text-muted-foreground bg-muted p-2 rounded-lg border border-border/50">
                    <Lock className="w-3 h-3" />
                    Connection details encrypted
                  </div>
                  <div className="text-[10px] text-muted-foreground/50">
                    Created {format(new Date(cred.createdAt!), 'MMM d, yyyy')}
                  </div>
                </div>
              </CardContent>
            </Card>
          ))
        )}
      </div>
    </div>
  );
}
