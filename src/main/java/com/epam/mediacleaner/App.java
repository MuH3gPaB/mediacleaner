package com.epam.mediacleaner;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CopyOnWriteArrayList;


public class App
{
	private static final String QUERY = "SELECT P_LOCATION FROM MEDIAS";

	public static void main(String[] args) throws SQLException, IOException
	{
		if (args.length < 4)
		{
			throw new IllegalArgumentException(
					"Wrong arguments. Expected [database url] [database login] [database pass] [media root path]");
		}

		String dataBaseUrl = args[0];
		String dataBaseLogin = args[1];
		String dataBasePass = args[2];
		String mediaRoot = args[3];

		try (Connection connection = DriverManager.getConnection(dataBaseUrl, dataBaseLogin, dataBasePass))
		{
			try (Statement st = connection.createStatement())
			{
				List<String> existFileNames = new ArrayList<>();
				ResultSet resultSet = st.executeQuery(QUERY);
				while (resultSet.next())
				{
					existFileNames.add(resultSet.getString(1));
				}

				Path mediaPath = Paths.get(mediaRoot);

				final List<Path> filesToRemove = new CopyOnWriteArrayList<>();

				Files.walkFileTree(mediaPath, new SimpleFileVisitor<Path>()
				{
					@Override
					public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException
					{
						path = mediaPath.relativize(path);
						String fileName = path.toString().replace("\\", "/");
						boolean found = true;
						if (!existFileNames.contains(fileName))
						{
							filesToRemove.add(path);
							found = false;
						}
						System.out.println("File " + fileName + " - " + (found ? "FOUND" : "NOT_FOUND"));

						return FileVisitResult.CONTINUE;
					}
				});

				notifyAndRemoveFiles(filesToRemove);
			}
		}

	}

	private static void notifyAndRemoveFiles(List<Path> filesToRemove)
	{
		Scanner sc = new Scanner(System.in);
		while (true)
		{
			System.out.println("Would you like to remove " + filesToRemove.size() + " files? Y/N/P(print)");
			String s = sc.nextLine();

			if ("Y".equals(s))
			{
				removeFiles(filesToRemove);
				return;
			}
			else if ("N".equals(s))
			{
				return;
			}
			else if ("P".equals(s))
			{
				System.out.println(filesToRemove);
			}
		}
	}

	private static void removeFiles(List<Path> filesToRemove)
	{
		System.out.println("Removing files...");
		filesToRemove.forEach(file -> {
			try
			{
				System.out.print("Removing " + file.toString());
				Files.deleteIfExists(file);
				System.out.println("  --- OK");
			}
			catch (IOException e)
			{
				System.out.println("Error removing file " + file.toString());
			}
		});
	}
}
