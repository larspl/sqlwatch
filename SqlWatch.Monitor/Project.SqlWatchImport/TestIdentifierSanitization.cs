using System;
using System.Text;

namespace SqlWatchImport
{
	class TestIdentifierSanitization
	{
		/// <summary>
		/// Sanitizes a string to be safe for use as a SQL identifier
		/// </summary>
		private static string SanitizeSqlIdentifier(string input)
		{
			if (string.IsNullOrWhiteSpace(input))
				return "unknown";

			// Remove common brackets and schema prefixes
			string cleaned = input.Replace("dbo.", "").Replace("[", "").Replace("]", "");
			
			// Build a safe identifier using only alphanumeric characters and underscores
			StringBuilder result = new StringBuilder();
			
			foreach (char c in cleaned)
			{
				if (char.IsLetterOrDigit(c))
				{
					result.Append(c);
				}
				else
				{
					result.Append('_');
				}
			}
			
			// Ensure it starts with a letter or underscore (SQL requirement)
			string final = result.ToString();
			if (final.Length > 0 && char.IsDigit(final[0]))
			{
				final = "_" + final;
			}
			
			// Limit length to avoid SQL identifier length limits (128 characters)
			if (final.Length > 50) // Leave room for prefixes and suffixes
			{
				final = final.Substring(0, 50);
			}
			
			return string.IsNullOrEmpty(final) ? "unknown" : final;
		}

		static void Main(string[] args)
		{
			// Test problematic identifiers from the screenshot
			Console.WriteLine("Testing SQL Identifier Sanitization:");
			Console.WriteLine("=====================================");
			
			string[] testCases = {
				"sqlwatch_meta_procedure",
				"SERVER\\INSTANCE",
				"server.domain.com",
				"server-name",
				"$6w19c$bsq1", // Example from the error
				"123server", // Starts with number
				"server with spaces",
				"[dbo].[table_name]",
				"very_long_server_name_that_exceeds_normal_sql_identifier_length_limits_and_should_be_truncated_properly",
				"",
				null
			};

			foreach (string test in testCases)
			{
				string result = SanitizeSqlIdentifier(test);
				Console.WriteLine($"Input: '{test}' -> Output: '{result}'");
			}

			// Test actual staging table name generation
			Console.WriteLine("\nTesting Staging Table Name Generation:");
			Console.WriteLine("======================================");
			
			string tableName = "sqlwatch_meta_procedure";
			string sqlInstance = "SERVER\\INSTANCE$6w19c$bsq1"; // Problematic name from error
			string safeInstanceName = SanitizeSqlIdentifier(sqlInstance);
			string safeTableName = SanitizeSqlIdentifier(tableName.Replace("dbo.", ""));
			string uniqueSuffix = $"{DateTime.Now:yyyyMMddHHmmss}_12345";
			
			string workingTableName = $"[#stg_{safeTableName}_{safeInstanceName}_{uniqueSuffix}]";
			
			Console.WriteLine($"Table: {tableName}");
			Console.WriteLine($"SQL Instance: {sqlInstance}");
			Console.WriteLine($"Safe Instance: {safeInstanceName}");
			Console.WriteLine($"Safe Table: {safeTableName}");
			Console.WriteLine($"Final Working Table Name: {workingTableName}");
			Console.WriteLine($"Length: {workingTableName.Length} characters");
		}
	}
}
