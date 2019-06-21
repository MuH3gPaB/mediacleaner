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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;


public class App
{
	static private int count = 0;

	private static final String QUERY = "SELECT P_LOCATION FROM medias";

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
				Set<String> knownFiles = new HashSet<>();
				ResultSet resultSet = st.executeQuery(QUERY);
				while (resultSet.next())
				{
					knownFiles.add(resultSet.getString(1));
				}

				Path mediaPath = Paths.get(mediaRoot);

				final Set<Path> existingFiles = new ConcurrentSkipListSet<>();

				final int total = knownFiles.size();

				Files.walkFileTree(mediaPath, new SimpleFileVisitor<Path>()
				{
					@Override
					public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException
					{
						existingFiles.add(path);
						System.out.print('\r');
						System.out.printf("%d/%d", ++count, total);

						return FileVisitResult.CONTINUE;
					}
				});

				Set<Path> filesToRemove = calculateFilesToRemove(knownFiles, existingFiles, mediaPath);

				notifyAndRemoveFiles(filesToRemove);
			}
		}

	}

	private static Set<Path> calculateFilesToRemove(Set<String> knownFiles, Set<Path> existingFiles, Path mediaPath)
	{
		return existingFiles.stream().filter(path -> !knownFiles.contains(mediaPath.relativize(path).toString())).collect(Collectors.toSet());
	}


	private static void notifyAndRemoveFiles(Collection<Path> filesToRemove)
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

	private static void removeFiles(Collection<Path> filesToRemove)
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
