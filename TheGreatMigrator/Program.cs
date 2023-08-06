using System;
using System.Data;
using System.IO;
using Oracle.ManagedDataAccess.Client;

namespace TheGreatMigrator {
    class Program {
        static void Main(string[] args) {
            string connectionString = ReadConnectionString();
            string executedScriptsFolder = ReadExecutedScriptsFolderPath();

            using (OracleConnection connection = new OracleConnection(connectionString)) {
                try {
                    connection.Open();
                    Console.WriteLine("Connected to the Oracle database.");

                    Console.WriteLine("\nChecking migration status...");
                    CheckMigrationStatus(connection);

                    Console.WriteLine("\nExecuting SQL scripts...");
                    ExecuteScripts(connection, executedScriptsFolder);

                    Console.WriteLine("Migration completed successfully.");
                } catch (Exception ex) {
                    Console.WriteLine("An error occurred: " + ex.Message);
                }
            }
        }
        static string ReadExecutedScriptsFolderPath() {
            string configFile = "./config.conf";
            string executedScriptsFolder = string.Empty;

            // Read the configuration file and find the executed scripts folder path
            using (StreamReader reader = new StreamReader(configFile)) {
                string line;
                while ((line = reader.ReadLine()) != null) {
                    string[] parts = line.Split('=');
                    if (parts.Length == 2) {
                        string key = parts[0].Trim();
                        string value = parts[1].Trim();

                        if (key == "executed_scripts_folder") {
                            executedScriptsFolder = value;
                            break;
                        }
                    }
                }
            }

            return executedScriptsFolder;
        }

        static void MoveExecutedScript(string scriptPath, string executedScriptsFolder) {
            string executedScriptPath = Path.Combine(executedScriptsFolder, Path.GetFileName(scriptPath));
            File.Move(scriptPath, executedScriptPath);
            Console.WriteLine("Moved executed script: " + scriptPath);
        }
        static string ReadConnectionString() {
            string configFile = "./config.conf";
            string host = "", port = "", service = "", user = "", password = "";

            using (StreamReader reader = new StreamReader(configFile)) {
                string line;
                while ((line = reader.ReadLine()) != null) {
                    string[] parts = line.Split('=');
                    if (parts.Length == 2) {
                        string key = parts[0].Trim();
                        string value = parts[1].Trim();
                        switch (key) {
                            case "host":
                                host = value;
                                break;
                            case "port":
                                port = value;
                                break;
                            case "service":
                                service = value;
                                break;
                            case "username":
                                user = value;
                                break;
                            case "password":
                                password = value;
                                break;
                        }
                    }
                }
            }

            string connectionString = $"Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={host})(PORT={port}))(CONNECT_DATA=(SERVICE_NAME={service})));User Id={user};Password={password};";
            return connectionString;
        }

        static void CheckMigrationStatus(OracleConnection connection) {
            using (OracleCommand command = connection.CreateCommand()) {
                command.CommandText = "SELECT script_file FROM migrations ORDER BY id DESC";
                using (OracleDataReader reader = command.ExecuteReader()) {
                    if (reader.Read()) {
                        Console.WriteLine("Last executed migration: " + reader.GetString(0));
                    } else {
                        Console.WriteLine("No migrations found.");
                    }
                }
            }
        }

        static void ExecuteScripts(OracleConnection connection, string executedScriptsFolder) {
            string scriptsFolder = "scripts";
            string[] scriptFiles = Directory.GetFiles(scriptsFolder, "*.sql");

            foreach (string scriptFile in scriptFiles) {
                string scriptContent = File.ReadAllText(scriptFile);

                using (OracleCommand command = connection.CreateCommand()) {
                    command.CommandText = "SELECT COUNT(*) FROM migrations WHERE script_file = :script_file";
                    command.Parameters.Add(":script_file", Path.GetFileName(scriptFile));
                    int count = Convert.ToInt32(command.ExecuteScalar());

                    if (count == 0) {
                        command.CommandText = scriptContent;
                        command.CommandType = CommandType.Text;

                        try {
                            command.ExecuteNonQuery();
                            command.CommandText = "INSERT INTO migrations (script_file, status) VALUES (:script_file, 'Success')";
                            command.Parameters.Add(":script_file", Path.GetFileName(scriptFile));
                            command.ExecuteNonQuery();
                            Console.WriteLine("Successfully executed migration: " + scriptFile);
                            MoveExecutedScript(scriptFile, executedScriptsFolder);
                        } catch (Exception ex) {
                            command.CommandText = "INSERT INTO migrations (script_file, status, error_message) VALUES (:script_file, 'Failed', :error_message)";
                            command.Parameters.Add(":script_file", Path.GetFileName(scriptFile));
                            command.Parameters.Add(":error_message", ex.Message);
                            command.ExecuteNonQuery();
                            Console.WriteLine("Failed to execute migration: " + scriptFile);
                            Console.WriteLine("Error: " + ex.Message);
                        }
                    } else {
                        Console.WriteLine("Skipping already executed migration: " + scriptFile);
                        MoveExecutedScript(scriptFile, executedScriptsFolder);
                    }
                }
            }
        }
    }
}
