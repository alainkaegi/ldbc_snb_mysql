/*
 * Copyright © 2017-2018 Alain Kägi
 */

package ldbc.loader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;
import java.util.Scanner;
import java.util.TimeZone;

import ldbc.utils.Configuration;
import ldbc.utils.Db;

/**
 * The BulkLoader class implements an application that loads an LDBC
 * Social Network Benchmark (SNB) dataset in a MySQL database.
 */
public class BulkLoader {

    // Suppress the default constructor.
    private BulkLoader() {}

    private static String progName = "BulkLoader";

    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    /**
     * Load an LDBC SNB dataset in a MySQL database.
     *
     * <p>A configuration file called <tt>params.ini</tt> specifies
     * the loading parameters.
     *
     * @param args  Unused
     */
    public static void main(String[] args) {

        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        timeFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        try {
            Configuration config = new Configuration();
            String url = "jdbc:mysql://" + config.host() + ":" + config.port() + "/" + config.database();
            Connection db = Db.connect(url, config.user(), config.password());
            purge(db, config.database());
            load(db, config.database(), config.datasetDirectory());
            createIndices(db, config.database());
        }
        catch (Configuration.ConfigurationFileNotFoundException e) {
            System.err.println(progName + ": " + e.getMessage());
            System.exit(1);
        }
        catch (Configuration.ConfigurationIOException e) {
            System.err.println(progName + ": " + e.getMessage());
            System.exit(1);
        }
        catch (Configuration.MissingConfigurationException e) {
            System.err.println(progName + ": " + e.getMessage());
            System.exit(1);
        }
        catch (FileNotFoundException e) {
            System.err.println(progName + ": " + e.getMessage());
            System.exit(1);
        }
        catch (IOException e) {
            System.err.println(progName + ": " + e.getMessage());
            System.exit(1);
        }
        catch (ParseException e) {
            System.err.println(progName + ": " + e.getMessage());
            System.exit(1);
        }
        catch (SQLException e) {
            System.err.println(progName + ": Error while loading data in the database: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void purge(Connection db, String database) throws SQLException {
        Statement stmt = db.createStatement();
        stmt.execute("DROP DATABASE IF EXISTS " + database);
        stmt.execute("CREATE DATABASE " + database);
        stmt.close();
    }

    private static void load(Connection db, String database, String datasetDirectory) throws IOException, FileNotFoundException, ParseException, SQLException {
        for (TableInformation table : tables) {
            System.out.println("Processing " + table.sourceFilename);
            createTable(db, database, table.name, table.structure);
            copyFileAndFilter(datasetDirectory + "/" + table.sourceFilename, "/var/lib/mysql-files/" + table.sourceFilename, table.filter);
            loadTable(db, database, table.name, "/var/lib/mysql-files/" + table.sourceFilename);
        }
    }

    private static void createTable(Connection db, String database, String name, String structure) throws SQLException {
        if (structure.length() == 0)
            return;
        Statement stmt = db.createStatement();
        stmt.executeUpdate("CREATE TABLE " + database + "."+ name + "(" + structure + ")");
        stmt.close();
    }

    private static void copyFile(String sourceFilename, String destinationFilename) throws FileNotFoundException, IOException {
        try (
            InputStream in = new FileInputStream(new File(sourceFilename));
            OutputStream out = new FileOutputStream(new File(destinationFilename));
        ) {
            byte[] buf = new byte[1024];
            int len;
            while ((len = in.read(buf)) > 0) {
                out.write(buf, 0, len);
            }
        }
    }

    private static void copyFileAndFilter(String sourceFilename, String destinationFilename, String filter) throws IOException,  FileNotFoundException,  ParseException {
        Pattern pattern = Pattern.compile(buildRegularExpression(filter));
        long lineNumber = 1;
        try (
            Scanner s = new Scanner(new File(sourceFilename));
            PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(destinationFilename)));
        ) {
            while (s.findInLine(pattern) != null) {
                MatchResult match = s.match();
                out.println(filter(filter, match, lineNumber == 1));
                s.nextLine();
                lineNumber++;
            }
        }
    }

    private static String buildRegularExpression(String filter) {
        int len = filter.replaceAll("0", "").length();
        StringBuilder sb = new StringBuilder();
        while (len > 1) {
            sb.append("(.*)\\|");
            len--;
        }
        sb.append("(.*)");
        return sb.toString();
    }

    private static String filter(String filter, MatchResult match, boolean isTitleLine) throws ParseException {
        if (isTitleLine)
            return filterTitleLine(filter, match);
        else
            return filterNonTitleLine(filter, match);
    }

    private static String filterTitleLine(String filter, MatchResult match) {
        int len = filter.length();
        char[] f = filter.toCharArray();
        int filterIndex = 0;
        int inputFieldIndex = 0;
        StringBuilder sb = new StringBuilder();
        boolean firstColumnIsSwapped = false;
        while (filterIndex < len) {
            switch (f[filterIndex]) {
            case 'C': // copy
            case 'T': // transform an ISO 8601 time into epoch
            case 'D': // transform a date into epoch
                sb.append(match.group(inputFieldIndex + 1));
                sb.append('|');
                inputFieldIndex++;
                break;
            case 'X': // delete
                inputFieldIndex++;
                break;
            case '0': // fill with nothing
                sb.append("nothing");
                sb.append('|');
                break;
            case 'S': // swap two neighboring columns
                if (firstColumnIsSwapped)
                    sb.append(match.group(inputFieldIndex));
                else
                    sb.append(match.group(inputFieldIndex + 2));
                sb.append('|');
                firstColumnIsSwapped = true;
                inputFieldIndex++;
                break;
            }
            filterIndex++;
        }
        return sb.toString();
    }

    private static String filterNonTitleLine(String filter, MatchResult match) throws ParseException {
        int len = filter.length();
        char[] f = filter.toCharArray();
        int filterIndex = 0;
        int inputFieldIndex = 0;
        StringBuilder sb = new StringBuilder();
        boolean firstColumnIsSwapped = false;
        while (filterIndex < len) {
            switch (f[filterIndex]) {
            case 'C': // copy
                sb.append(match.group(inputFieldIndex + 1));
                sb.append('|');
                inputFieldIndex++;
                break;
            case 'X': // delete
                inputFieldIndex++;
                break;
            case 'T': // transform an ISO 8601 time into epoch
                sb.append(timeFormat.parse(match.group(inputFieldIndex + 1)).getTime());
                sb.append('|');
                inputFieldIndex++;
                break;
            case 'D': // transform a date into epoch
                sb.append(dateFormat.parse(match.group(inputFieldIndex + 1)).getTime());
                sb.append('|');
                inputFieldIndex++;
                break;
            case '0': // fill with nothing
                sb.append('|');
                break;
            case 'S': // swap two neighboring columns
                if (firstColumnIsSwapped)
                    sb.append(match.group(inputFieldIndex));
                else
                    sb.append(match.group(inputFieldIndex + 2));
                sb.append('|');
                firstColumnIsSwapped = true;
                inputFieldIndex++;
                break;
            }
            filterIndex++;
        }
        return sb.toString();
    }

    private static void loadTable(Connection db, String database, String name, String sourceFilename) throws SQLException {
        Statement stmt = db.createStatement();
        stmt.executeUpdate("LOAD DATA INFILE '" + sourceFilename + "' INTO TABLE " + database + "." + name + " FIELDS TERMINATED by '|' IGNORE 1 LINES");
        stmt.close();
    }

    private static void createIndices(Connection db, String database) throws SQLException {
        for (IndexInformation index : indices) {
            System.out.println("Creating " + index.indexName + " index");
            createIndex(db, database, index.indexName, index.tableName, index.columnName);
        }
    }

    private static void createIndex(Connection db, String database, String indexName, String tableName, String columnName) throws SQLException {
        Statement stmt = db.createStatement();
        stmt.executeUpdate("CREATE INDEX " + indexName + " ON " + database + "." + tableName + "(" + columnName + ")");
        stmt.close();
    }

    private static class TableInformation {
        String name;
        String structure;
        String sourceFilename;
        String filter; // 'C' = copy, 'X' = delete, 'T' = time ISO 8601 to epoch translation, 'D' = date to epoch translation, '0' = nothing, 'S' = swap two neighboring columns
        private TableInformation(String name, String structure, String filename, String filter) {
            this.name = name;
            this.structure = structure;
            this.sourceFilename = filename;
            this.filter = filter;
        }
    }

    static TableInformation[] tables = {
        // Entities
        new TableInformation(
            "Message",
            "id BIGINT NOT NULL, " +
                "imageFile VARCHAR(40), " +
                "creationDate BIGINT, " +
                "locationIP VARCHAR(40), " +
                "browserUsed VARCHAR(40), " +
                "language VARCHAR(40), " +
                "content VARCHAR(2100), " +
                "length INT, " +
                "PRIMARY KEY (id)",
            "comment_0_0.csv",
            "C0TCC0CC"
        ),
        new TableInformation(
            "Forum",
            "id BIGINT NOT NULL, " +
                "title VARCHAR(100), " +
                "creationDate BIGINT, " +
                "PRIMARY KEY (id)",
            "forum_0_0.csv",
            "CCT"
        ),
        new TableInformation(
            "Organisation",
            "id BIGINT NOT NULL, " +
                "name VARCHAR(150), " +
                "PRIMARY KEY (id)",
            "organisation_0_0.csv",
            "CXCX"
        ),
        new TableInformation(
            "Person",
            "id BIGINT NOT NULL, " +
                "firstName VARCHAR(40), " +
                "lastName VARCHAR(40), " +
                "gender VARCHAR(40), " +
                "birthday BIGINT, " +
                "creationDate BIGINT, " +
                "locationIP VARCHAR(40), " +
                "browserUsed VARCHAR(40), " +
                "PRIMARY KEY (id)",
            "person_0_0.csv",
            "CCCCDTCC"
        ),
        new TableInformation(
            "Place",
            "id BIGINT NOT NULL, " +
                "name VARCHAR(100), " +
                "PRIMARY KEY (id)",
            "place_0_0.csv",
            "CCXX"
        ),
        new TableInformation(
            "Message",
            "", // Merge Post and Comment into Message
            "post_0_0.csv",
            "CCTCCCCC"
        ),
        new TableInformation(
            "Tag",
            "id BIGINT NOT NULL, " +
                "name VARCHAR(100), " +
                "PRIMARY KEY (id)",
            "tag_0_0.csv",
            "CCX"
        ),
        new TableInformation(
            "TagClass",
            "id BIGINT NOT NULL, " +
                "name VARCHAR(40), " +
                "PRIMARY KEY (id)",
            "tagclass_0_0.csv",
            "CCX"
        ),

        // Relationships
        new TableInformation(
            "MessageHasCreatorPerson",
            "messageId BIGINT, " +
                "personId BIGINT",
            "comment_hasCreator_person_0_0.csv",
            "CC"
        ),
        new TableInformation(
            "CommentHasTagTag",
            "commentId BIGINT, " +
                "tagId BIGINT",
            "comment_hasTag_tag_0_0.csv",
            "CC"
        ),
        new TableInformation(
            "CommentIsLocatedInPlace",
            "commentId BIGINT, " +
                "placeId BIGINT",
            "comment_isLocatedIn_place_0_0.csv",
            "CC"
        ),
        new TableInformation(
            "CommentReplyOfMessage",
            "commentId BIGINT, " +
                "messageId BIGINT",
            "comment_replyOf_comment_0_0.csv",
            "CC"
        ),
        new TableInformation(
            "CommentReplyOfMessage",
            "", // Merge CommentReplyOfPost and CommentReplyOfComment into CommentReplyOfMessage
            "comment_replyOf_post_0_0.csv",
            "CC"
        ),
        new TableInformation(
            "ForumContainerOfPost",
            "forumId BIGINT, " +
                "postId BIGINT",
            "forum_containerOf_post_0_0.csv",
            "CC"
        ),
        new TableInformation(
            "ForumHasMemberPerson",
            "forumId BIGINT, " +
                "personId BIGINT, " +
                "joinDate BIGINT",
            "forum_hasMember_person_0_0.csv",
            "CCT"
        ),
        new TableInformation(
            "ForumHasModeratorPerson",
            "forumId BIGINT, " +
                "personId BIGINT",
            "forum_hasModerator_person_0_0.csv",
            "CC"
        ),
        new TableInformation(
            "ForumHasTagTag",
            "forumId BIGINT, " +
                "tagId BIGINT",
            "forum_hasTag_tag_0_0.csv",
            "CC"
        ),
        new TableInformation(
            "OrganisationIsLocatedInPlace",
            "organisationId BIGINT, " +
                "placeId BIGINT",
            "organisation_isLocatedIn_place_0_0.csv",
            "CC"
        ),
        new TableInformation(
            "PersonEmailEmailAddress",
            "personId BIGINT, " +
                "email VARCHAR(80)",
            "person_email_emailaddress_0_0.csv",
            "CC"
        ),
        new TableInformation(
            "PersonHasInterestTag",
            "personId BIGINT, " +
                "tagId BIGINT",
            "person_hasInterest_tag_0_0.csv",
            "CC"
        ),
        new TableInformation(
            "PersonIsLocatedInPlace",
            "personId BIGINT, " +
                "placeId BIGINT",
            "person_isLocatedIn_place_0_0.csv",
            "CC"
        ),
        new TableInformation(
            "PersonKnowsPerson",
            "person1Id BIGINT, " +
                "person2Id BIGINT, " +
                "creationDate BIGINT",
            "person_knows_person_0_0.csv",
            "CCT"
        ),
        new TableInformation( // load relationship again, but with the first two columns swapped
            "PersonKnowsPerson",
            "",
            "person_knows_person_0_0.csv",
            "SST"
        ),
        new TableInformation(
            "PersonLikesComment",
            "personId BIGINT, " +
                "commentId BIGINT, " +
                "creationDate BIGINT",
            "person_likes_comment_0_0.csv",
            "CCT"
        ),
        new TableInformation(
            "PersonLikesPost",
            "personId BIGINT, " +
                "postId BIGINT, " +
                "creationDate BIGINT",
            "person_likes_post_0_0.csv",
            "CCT"
        ),
        new TableInformation(
            "PersonSpeaksLanguage",
            "personId BIGINT, " +
                "language VARCHAR(40)",
            "person_speaks_language_0_0.csv",
            "CC"
        ),
        new TableInformation(
            "PersonStudyAtOrganisation",
            "personId BIGINT, " +
                "organisationId BIGINT, " +
                "classYear INT",
            "person_studyAt_organisation_0_0.csv",
            "CCC"
        ),
        new TableInformation(
            "PersonWorkAtOrganisation",
            "personId BIGINT, " +
                "organisationId BIGINT, " +
                "workFrom INT",
            "person_workAt_organisation_0_0.csv",
            "CCC"
        ),
        new TableInformation(
            "PlaceIsPartOfPlace",
            "place1Id BIGINT, " +
                "place2Id BIGINT",
            "place_isPartOf_place_0_0.csv",
            "CC"
        ),
        new TableInformation(
            "MessageHasCreatorPerson",
            "", // Merge PostHasCreatorPerson and CommentHasCreatorPerson into MessageHasCreatorPerson
            "post_hasCreator_person_0_0.csv",
            "CC"
        ),
        new TableInformation(
            "PostHasTagTag",
            "postId BIGINT, " +
                "tagId BIGINT",
            "post_hasTag_tag_0_0.csv",
            "CC"
        ),
        new TableInformation(
            "PostIsLocatedInPlace",
            "postId BIGINT, " +
                "placeId BIGINT",
            "post_isLocatedIn_place_0_0.csv",
            "CC"
        ),
        new TableInformation(
            "TagHasTypeTagClass",
            "tagId BIGINT, " +
                "tagClassId BIGINT",
            "tag_hasType_tagclass_0_0.csv",
            "CC"
        ),
        new TableInformation(
            "TagClassIsSubclassOfTagClass",
            "tagClass1Id BIGINT, " +
                "tagClass2Id BIGINT",
            "tagclass_isSubclassOf_tagclass_0_0.csv",
            "CC"
        )
    };

    private static class IndexInformation {
        String indexName;
        String tableName;
        String columnName;
        private IndexInformation(String indexName, String tableName, String columnName) {
            this.indexName = indexName;
            this.tableName = tableName;
            this.columnName = columnName;
        }
    }

    static IndexInformation[] indices = {
        new IndexInformation(
            "TagName",
            "Tag",
            "name"
        ),
        new IndexInformation(
            "KnowsPerson1Id",
            "PersonKnowsPerson",
            "person1Id"
        ),
        new IndexInformation(
            "KnowsPerson2Id",
            "PersonKnowsPerson",
            "person2Id"
        ),
        new IndexInformation(
            "HasCreatorMessageId",
            "MessageHasCreatorPerson",
            "messageId"
        ),
        new IndexInformation(
            "HasCreatorPersonId",
            "MessageHasCreatorPerson",
            "personId"
        ),
        new IndexInformation(
            "ReplyOfMessageId",
            "CommentReplyOfMessage",
            "messageId"
        ),
        new IndexInformation(
            "ReplyOfCommentId",
            "CommentReplyOfMessage",
            "commentId"
        ),
        new IndexInformation(
            "HasTagPostId",
            "PostHasTagTag",
            "postId"
        ),
        new IndexInformation(
            "HasTagTagId",
            "PostHasTagTag",
            "tagId"
        ),
        new IndexInformation(
            "HasMemberPersonId",
            "ForumHasMemberPerson",
            "personId"
        ),
        new IndexInformation(
            "ContainerOfPostId",
            "ForumContainerOfPost",
            "postId"
        ),
        new IndexInformation(
            "LikesPostId",
            "PersonLikesPost",
            "postId"
        ),
        new IndexInformation(
            "LikesCommentId",
            "PersonLikesComment",
            "commentId"
        ),
        new IndexInformation(
            "IsLocatedInPersonId",
            "PersonIsLocatedInPlace",
            "personId"
        ),
        new IndexInformation(
            "IsLocatedInPlaceId",
            "PersonIsLocatedInPlace",
            "placeId"
        ),
        new IndexInformation(
            "HasTypeTagId",
            "TagHasTypeTagClass",
            "tagId"
        ),
        new IndexInformation(
            "EmailPersonId",
            "PersonEmailEmailAddress",
            "personId"
        ),
        new IndexInformation(
            "SpeaksPersonId",
            "PersonSpeaksLanguage",
            "personId"
        ),
        new IndexInformation(
            "StudyAtPersonId",
            "PersonStudyAtOrganisation",
            "personId"
        ),
        new IndexInformation(
            "WorkAtPersonId",
            "PersonWorkAtOrganisation",
            "personId"
        ),
        new IndexInformation(
            "IsLocatedOrganisationId",
            "OrganisationIsLocatedInPlace",
            "organisationId"
        ),
        new IndexInformation(
            "ContainerOfForumId",
            "ForumContainerOfPost",
            "forumId"
        ),
        new IndexInformation(
            "IsLocatedInPostId",
            "PostIsLocatedInPlace",
            "postId"
        ),
        new IndexInformation(
            "IsLocatedInCommentId",
            "CommentIsLocatedInPlace",
            "commentId"
        ),
        new IndexInformation(
            "IsPartOfPlace1Id",
            "PlaceIsPartOfPlace",
            "place1Id"
        ),
    };

}
