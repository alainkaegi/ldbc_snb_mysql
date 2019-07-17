/**
 * LDBC common queries.
 *
 * The callers are responsible for setting up the JDBC interface
 * properly (e.g., disabling auto commit) if these queries are used in
 * the context of a larger transaction.
 *
 * Copyright © 2018, 2019 Alain Kägi
 */

package ldbc.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LdbcUtils {

    // Simple queries involving a single entry in a table.

    //  Entities.

    //   Messages.

    /**
     * Get the creation date of the specified message.
     * @param c          A database connection
     * @param messageId  The message's unique identifier
     * @return the creation date of the given message or -1 if not found
     * @throws SQLException if a database access error occurs
     */
    static public long getCreationDate(Connection c, long messageId) throws SQLException {
        long date = -1;
        String dateQuery =
            "  SELECT Message.creationDate " +
            "    FROM Message " +
            "   WHERE Message.id = " + messageId;
        try (Statement s = c.createStatement();
             ResultSet r = s.executeQuery(dateQuery)) {
            if (r.next())
                date = r.getLong("Message.creationDate");
        }
        return date;
    }

    /**
     * Get the content (or image file) of the specified message.
     * @param c          A database connection
     * @param messageId  The message's unique identifier
     * @return the content associated with given message or null if not found
     * @throws SQLException if a database access error occurs
     */
    static public String getContent(Connection c, long messageId) throws SQLException {
        String content = null;
        String contentQuery =
            "  SELECT Message.content, Message.imageFile " +
            "    FROM Message " +
            "   WHERE Message.id = " + messageId;
        try (Statement s = c.createStatement();
             ResultSet r = s.executeQuery(contentQuery)) {
            if (r.next()) {
                content = r.getString("Message.content");
                if (content.length() == 0)
                    content = r.getString("Message.imageFile");
            }
        }
        return content;
    }

    //   Forums.

    /**
     * Get the title of the specified forum.
     * @param c        A database connection
     * @param forumId  The forum's unique identifier
     * @return the title of the given forum or null if not found
     * @throws SQLException if a database access error occurs
     */
    static public String getForumTitle(Connection c, long forumId) throws SQLException {
        String forumTitle = null;
        String forumQuery =
            "  SELECT Forum.title " +
            "    FROM Forum " +
            "   WHERE Forum.id = " + forumId;
        try (Statement s = c.createStatement();
             ResultSet r = s.executeQuery(forumQuery)) {
            if (r.next())
                forumTitle = r.getString("Forum.title");
        }
        return forumTitle;
    }

    //   Persons.

    /**
     * Get the first name of the specified person.
     * @param c         A database connnection
     * @param personId  The person's unique identifier
     * @return the first name of the given person or null if not found
     * @throws SQLException if a database access error occurs
     */
    static public String getFirstName(Connection c, long personId) throws SQLException {
        String firstName = null;
        String firstNameQuery =
            "  SELECT Person.firstName " +
            "    FROM Person " +
            "   WHERE Person.id = " + personId;
        try (Statement s = c.createStatement();
             ResultSet r = s.executeQuery(firstNameQuery)) {
            if (r.next())
                firstName = r.getString("Person.firstName");
        }
        return firstName;
    }

    /**
     * Get the last name of the specified person.
     * @param c         A database connection
     * @param personId  The person's unique identifier
     * @return the last name of the given person or null if not found
     * @throws SQLException if a database access error occurs
     */
    static public String getLastName(Connection c, long personId) throws SQLException {
        String lastName = null;
        String lastNameQuery =
            "  SELECT Person.lastName " +
            "    FROM Person " +
            "   WHERE Person.id = " + personId;
        try (Statement s = c.createStatement();
             ResultSet r = s.executeQuery(lastNameQuery)) {
            if (r.next())
                lastName = r.getString("Person.lastName");
        }
        return lastName;
    }

    /**
     * Get the gender of the specified person.
     * @param c         A database connection
     * @param personId  The person's unique identifier
     * @return the gender of the given person or null if not found
     * @throws SQLException if a database access error occurs
     */
    static public String getGender(Connection c, long personId) throws SQLException {
        String gender = null;
        String genderQuery =
            "  SELECT Person.gender " +
            "    FROM Person " +
            "   WHERE Person.id = " + personId;
        try (Statement s = c.createStatement();
             ResultSet r = s.executeQuery(genderQuery)) {
            if (r.next())
                gender = r.getString("Person.gender");
        }
        return gender;
    }

    //   Places.

    /**
     * Get the identifier of the specified country.
     * @param c        A database connection
     * @param country  The country's name
     * @return the identifier of the given country or -1 if not found
     * @throws SQLException if a database access error occurs
     * Some towns have the same name as a country.  LEFT JOIN Place
     * twice with PlaceIsPartOfPlace.  Only countries will have the
     * first version PlaceIsPartOfPlace's fields set to non-NULL
     * values and the second version PlaceIsPartOfPlace's fields set
     * to NULL.
     */
    static public long getCountryId(Connection c, String country) throws SQLException {
        long countryId = -1;
        String countryQuery =
            "   SELECT Place.id " +
            "     FROM Place " +
            "LEFT JOIN PlaceIsPartOfPlace AS P " +
            "       ON Place.id = P.place1Id " +
            "LEFT JOIN PlaceIsPartOfPlace AS R " +
            "       ON P.place2Id = R.place1Id " +
            "    WHERE Place.name = \"" + country + "\"" +
            "      AND P.place1Id IS NOT NULL" +
            "      AND R.place1Id IS NULL";
        try (Statement s = c.createStatement();
             ResultSet r = s.executeQuery(countryQuery)) {
            if (r.next())
                countryId = r.getLong("Place.id");
        }
        return countryId;
    }

    //   Tags.

    /**
     * Get the name the specified tag.
     * @param c      A database connection
     * @param tagId  The tag's unique identifier
     * @return the name of the given tag or null if not found
     * @throws SQLException if a database access error occurs
     */
    static public String getTagName(Connection c, long tagId) throws SQLException {
        String tagName = null;
        String tagQuery =
            "  SELECT Tag.name " +
            "    FROM Tag " +
            "   WHERE Tag.id = " + tagId;
        try (Statement s = c.createStatement();
             ResultSet r = s.executeQuery(tagQuery)) {
            if (r.next())
                tagName = r.getString("Tag.name");
        }
        return tagName;
    }

    /**
     * Get the identifier of the specified tag.
     * @param c    A database connection
     * @param tag  The tag's name
     * @return the identifier of the given tag or -1 if not found
     * @throws SQLException if a database access error occurs
     */
    static public long getTagId(Connection c, String tag) throws SQLException {
        long tagId = -1;
        String tagQuery =
            "  SELECT Tag.id " +
            "    FROM Tag " +
            "   WHERE Tag.name = \"" + tag + "\"";
        try (Statement s = c.createStatement();
             ResultSet r = s.executeQuery(tagQuery)) {
            if (r.next())
                tagId = r.getLong("Tag.id");
        }
        return tagId;
    }

    //   Tag Classes.

    /**
     * Return the identifier of the given tag class.
     * @param c         A database connection
     * @param tagClass  The tag class name
     * @return the identifier of the given tag class or -1 if not found
     * @throws SQLException if a database access error occurs
     */
    static public long getTagClassId(Connection c, String tagClass) throws SQLException {
        long tagClassId = -1;
        String tagClassIdQuery =
            "  SELECT TagClass.id " +
            "    FROM TagClass " +
            "   WHERE TagClass.name = \"" + tagClass + "\"";
        try (Statement s = c.createStatement();
             ResultSet r = s.executeQuery(tagClassIdQuery)) {
            if (r.next())
                tagClassId = r.getLong("TagClass.id");
        }
        return tagClassId;
    }

    //  Relationships.

    //   Has Creator.

    /**
     * Find all messages created by the given person.
     * @param c         A database connection
     * @param personId  The person's unique identifier
     * @return a (possibly empty) list of all messages created by the given person
     * @throws SQLException if a database access error occurs
     */
    static public List<Long> getMessagesCreatedBy(Connection c, long personId) throws SQLException {
        List<Long> messages = new ArrayList<>();
        String messageQuery =
            "  SELECT MessageHasCreatorPerson.messageId " +
            "    FROM MessageHasCreatorPerson " +
            "   WHERE MessageHasCreatorPerson.personId = " + personId;
        try (Statement s = c.createStatement();
             ResultSet r = s.executeQuery(messageQuery)) {
            while (r.next())
                messages.add(r.getLong("MessageHasCreatorPerson.messageId"));
        }
        return messages;
    }

    /**
     * Get the author of the given message.
     * @param c          A database connection
     * @param messageId  The message's unique identifier
     * @return the author of the given message or -1 if not found
     * @throws SQLException if a database access error occurs
     */
    static public long getAuthorOf(Connection c, long messageId) throws SQLException {
        long authorId = -1;
        String authorQuery =
            "  SELECT MessageHasCreatorPerson.personId " +
            "    FROM MessageHasCreatorPerson " +
            "   WHERE MessageHasCreatorPerson.messageId = " + messageId;
        try (Statement s = c.createStatement();
             ResultSet r = s.executeQuery(authorQuery)) {
            if (r.next())
                authorId = r.getLong("MessageHasCreatorPerson.personId");
        }
        return authorId;
    }

    //   Reply Of.

    /**
     * Return the parent of the given message.
     * @param c          A database connection
     * @param messageId  The message's unique identifier
     * @return the identifier of the parent of the given message or -1 if not found
     * @throws SQLException if a database access error occurs
     * A post does not have a parent.
     */
    static public long getParentMessageId(Connection c, long messageId) throws SQLException {
        long parentMessageId = -1;
        String parentMessageQuery =
            "  SELECT CommentReplyOfMessage.messageId " +
            "    FROM CommentReplyOfMessage " +
            "   WHERE CommentReplyOfMessage.commentId = " + messageId;
        try (Statement s = c.createStatement();
             ResultSet r = s.executeQuery(parentMessageQuery)) {
            if (r.next())
                parentMessageId = r.getLong("CommentReplyOfMessage.messageId");
        }
        return parentMessageId;
    }

    /**
     * Return the original post of the given message.
     *
     * A message is either a post or a comment; a comment is either a
     * reply to a post or a reply to a comment.  This function returns
     * the original post associated with the given message, following
     * the Reply Of relationship.  If the given message is a post,
     * this function returns that post.
     * @param c          A database connection
     * @param messageId  The message's unique identifier
     * @return the identifier of the original post associated with the given message
     * @throws SQLException if a database access error occurs
     */
    static public long getParentPostId(Connection c, long messageId) throws SQLException {
        long parentPostId;
        long nextId = messageId;
        do {
            parentPostId = nextId;
            nextId = getParentMessageId(c, parentPostId);
        } while (nextId != -1);
        return parentPostId;
    }

    /**
     * Return true if the message is a post.
     * @param c          A database connection
     * @param messageId  The message's unique identifier
     * @return true if the message is a post
     * @throws SQLException if a database access error occurs
     * The function assumes that the message exists; it will return
     * true as well of it doesn't.
     */
    static public boolean isMessageAPost(Connection c, long messageId) throws SQLException {
        return getParentMessageId(c, messageId) == -1;
    }

    //   Container Of.

    /**
     * Return the forum containing the given post.
     * @param c       A database connection
     * @param postId  The post's unique identifier
     * @return the identifier of the forum containing the given message or -1 if not found
     * @throws SQLException if a database access error occurs
     */
    static public long getForumOfPost(Connection c, long postId) throws SQLException {
        long forumId = -1;
        String forumQuery =
            "   SELECT ForumContainerOfPost.forumId " +
            "     FROM ForumContainerOfPost " +
            "    WHERE ForumContainerOfPost.postId = " + postId;
        try (Statement s = c.createStatement();
             ResultSet r = s.executeQuery(forumQuery)) {
            if (r.next())
                forumId = r.getLong("ForumContainerOfPost.forumId");
        }
        return forumId;
    }

    //   Email.

    /**
     * Get the email addresses of the specified person.
     * @param c         A database connection
     * @param personId  The person's unique identifier
     * @return a (possibly empty) list of emails associated with the given person
     * @throws SQLException if a database access error occurs
     */
    static public List<String> getEmails(Connection c, long personId) throws SQLException {
        List<String> emails = new ArrayList<>();
        String emailQuery =
            "  SELECT PersonEmailEmailAddress.email " +
            "    FROM PersonEmailEmailAddress " +
            "   WHERE PersonEmailEmailAddress.personId = " + personId + " " +
            "ORDER BY PersonEmailEmailAddress.email";
        try (Statement s = c.createStatement();
             ResultSet r = s.executeQuery(emailQuery)) {
            while (r.next())
                emails.add(r.getString("PersonEmailEmailAddress.email"));
        }
        return emails;
    }

    //   HasInterest

    /**
     * Get the tags in which the specified person has interest.
     * @param c         A database connection
     * @param personId  The person's unique identifier
     * @return a (possibly empty) set of tags associated with the given person
     * @throws SQLException if a database access error occurs
     */
    static public Set<Long> getTags(Connection c, long personId) throws SQLException {
        Set<Long> tags = new HashSet<>();
        String tagQuery =
            "  SELECT PersonHasInterestTag.tagId " +
            "    FROM PersonHasInterestTag " +
            "   WHERE PersonHasInterestTag.personId = " + personId;
        try (Statement s = c.createStatement();
             ResultSet r = s.executeQuery(tagQuery)) {
            while (r.next())
                tags.add(r.getLong("PersonHasInterestTag.tagId"));
        }
        return tags;
    }

    //   Knows.

    /**
     * Return true if two persons know each other.
     * @param c          A database connection
     * @param person1Id  One person's unique identifier
     * @param person2Id  Another person's unique identifier
     * @return true if the two persons are friend.
     * @throws SQLException if a database access error occurs
     */
    static public boolean areTheyFriend(Connection c, long person1Id, long person2Id) throws SQLException {
        boolean areTheyFriend = false;
        String friendQuery =
            "  SELECT PersonKnowsPerson.person1Id " +
            "    FROM PersonKnowsPerson " +
            "   WHERE PersonKnowsPerson.person1Id = " + person1Id +
            "     AND PersonKnowsPerson.person2Id = " + person2Id;
        try (Statement s = c.createStatement();
             ResultSet r = s.executeQuery(friendQuery)) {
            if (r.next())
                areTheyFriend = true;
        }
        return areTheyFriend;
    }

    /**
     * Find the given person's friends.
     * @param c         A database connection
     * @param personId  The person's unique identifier
     * @return a list of the person's friends
     * @throws SQLException if a database access error occurs
     */
    public static List<Long> findFriends(Connection c, long personId) throws SQLException {
        List<Long> friends = new ArrayList<>();
        String friendQuery =
            "   SELECT PersonKnowsPerson.person2Id " +
            "     FROM PersonKnowsPerson " +
            "    WHERE PersonKnowsPerson.person1Id = " + personId;
        try (Statement s = c.createStatement();
             ResultSet r = s.executeQuery(friendQuery)) {
            while (r.next())
                friends.add(r.getLong("PersonKnowsPerson.person2Id"));
        }
        return friends;
    }

    //   Speaks.

    /**
     * Get the languages spoken by the specified person.
     * @param c         A database connection
     * @param personId  The person's unique identifier
     * @return a (possibly empty) list of languages spoken by the given person
     * @throws SQLException if a database access error occurs
     */
    static public List<String> getLanguages(Connection c, long personId) throws SQLException {
        List<String> languages = new ArrayList<>();
        String languageQuery =
            "  SELECT PersonSpeaksLanguage.language " +
            "    FROM PersonSpeaksLanguage " +
            "   WHERE PersonSpeaksLanguage.personId = " + personId;
        try (Statement s = c.createStatement();
             ResultSet r = s.executeQuery(languageQuery)) {
            while (r.next())
                languages.add(r.getString("PersonSpeaksLanguage.language"));
        }
        return languages;
    }

    //   Post Has Tag.

    /**
     * Return true if the specified post contains the given tag.
     * @param c       A database connection
     * @param postId  The post's unique identifier
     * @param tagId   The tag's unique identifier
     * @return true if the given post contains the given tag
     * @throws SQLException if a database access error occurs
     */
    static public boolean doesPostHaveTag(Connection c, long postId, long tagId) throws SQLException {
        boolean doesPostHaveTag = false;
        String tagQuery =
            "  SELECT PostHasTagTag.tagId " +
            "    FROM PostHasTagTag " +
            "   WHERE PostHasTagTag.tagId = " + tagId + " " +
            "     AND PostHasTagTag.postId = " + postId;
        try (Statement s = c.createStatement();
             ResultSet r = s.executeQuery(tagQuery)) {
            if (r.next())
                doesPostHaveTag = true;
        }
        return doesPostHaveTag;
    }

    //   Has Type.

    /**
     * Return the tag class of the given tag.
     * @param c      A database connection
     * @param tagId  The tag's unique identifier
     * @return the tag class of the given tag
     * @throws SQLException if a database access error occurs
     */
    static public long getTypeTagClassIdOf(Connection c, long tagId) throws SQLException {
        long tagClassId = 0;
        String tagClassQuery =
            "  SELECT TagHasTypeTagClass.tagClassId " +
            "    FROM TagHasTypeTagClass " +
            "   WHERE TagHasTypeTagClass.tagId = " + tagId;
        try (Statement s = c.createStatement();
             ResultSet r = s.executeQuery(tagClassQuery)) {
            if (r.next())
                tagClassId = r.getLong("TagHasTypeTagClass.tagClassId");
        }
        return tagClassId;
    }

    //   Is Subclass Of.

    /**
     * Return true if tag class is subclass of target tag class.
     * @param c                 A database connection
     * @param tagClassId        Tag class's unique identifier
     * @param targetTagClassId  Target class's unique identifier
     * @return true if tag class is target tag class or one of its subclasses
     * @throws SQLException if a database access error occurs
     */
    static public boolean isTagClassSubclassOfTagClass(Connection c, long tagClassId, long targetTagClassId) throws SQLException {
        boolean isSubclass = false;
        String subClassQuery =
            "  SELECT TagClassIsSubclassOfTagClass.tagClass2Id " +
            "    FROM TagClassIsSubclassOfTagClass " +
            "   WHERE TagClassIsSubclassOfTagClass.tagClass1Id = " + tagClassId;
        try (Statement s = c.createStatement();
             ResultSet r = s.executeQuery(subClassQuery)) {
            if (r.next()) {
                long overclassId = r.getLong("TagClassIsSubclassOfTagClass.tagClass2Id");
                if (overclassId == targetTagClassId)
                    isSubclass = true;
                else
                    isSubclass = isTagClassSubclassOfTagClass(c, overclassId, targetTagClassId);
            }
        }
        return isSubclass;
    }

    // More complex queries involving more than one table.

    /**
     * Find the location of the specified person.
     * @param c         A database connection
     * @param personId  The person's unique identifier
     * @return the current location of the given person or null if not found
     * @throws SQLException if a database access error occurs
     */
    static public String findPlace(Connection c, long personId) throws SQLException {
        String place = null;
        String placeQuery =
            "  SELECT Place.name " +
            "    FROM PersonIsLocatedInPlace, Place " +
            "   WHERE PersonIsLocatedInPlace.personId = " + personId + " " +
            "     AND PersonIsLocatedInPlace.placeId = Place.id";
        try (Statement s = c.createStatement();
             ResultSet r = s.executeQuery(placeQuery)) {
            if (r.next())
                place = r.getString("Place.name");
        }
        return place;
    }

    /**
     * Find the schools attended by the specified person.
     * @param c         A database connection
     * @param personId  The person's unique identifier
     * @return a (possibly empty) list of schools attended by the given person
     * @throws SQLException if a database access error occurs
     */
    static public List<List<Object>> findSchools(Connection c, long personId) throws SQLException {
        List<List<Object>> schools = new ArrayList<>();
        String schoolQuery =
            "  SELECT Organisation.name, " +
            "    PersonStudyAtOrganisation.classYear, " +
            "    Place.name " +
            "    FROM PersonStudyAtOrganisation, Organisation, " +
            "         Place, OrganisationIsLocatedInPlace " +
            "   WHERE PersonStudyAtOrganisation.personId = " + personId + " " +
            "     AND PersonStudyAtOrganisation.organisationId = Organisation.id " +
            "     AND OrganisationIsLocatedInPlace.organisationId = Organisation.id " +
            "     AND OrganisationIsLocatedInPlace.placeId = Place.id";
        try (Statement s = c.createStatement();
             ResultSet r = s.executeQuery(schoolQuery)) {
            while (r.next()) {
                List<Object> school = new ArrayList<>();
                school.add(r.getString("Organisation.name"));
                school.add(r.getInt("PersonStudyAtOrganisation.classYear"));
                school.add(r.getString("Place.name"));
                schools.add(school);
            }
        }
        return schools;
    }

    /**
     * Find the organizations by the specified person.
     * @param c         A database connection
     * @param personId  The specified person's unique identifier
     * @return a (possibly empty) list of schools attended by the given person
     * @throws SQLException if a database access error occurs
     */
    static public List<List<Object>> findOrganizations(Connection c, long personId) throws SQLException {
        List<List<Object>> organizations = new ArrayList<>();
        String organizationQuery =
            "  SELECT Organisation.name, " +
            "    PersonWorkAtOrganisation.workFrom, " +
            "    Place.name " +
            "    FROM PersonWorkAtOrganisation, Organisation, " +
            "         Place, OrganisationIsLocatedInPlace " +
            "   WHERE PersonWorkAtOrganisation.personId = " + personId + " " +
            "     AND PersonWorkAtOrganisation.organisationId = Organisation.id " +
            "     AND OrganisationIsLocatedInPlace.organisationId = Organisation.id " +
            "     AND OrganisationIsLocatedInPlace.placeId = Place.id";
        try (Statement s = c.createStatement();
             ResultSet r = s.executeQuery(organizationQuery)) {
            while (r.next()) {
                List<Object> organization = new ArrayList<>();
                organization.add(r.getString("Organisation.name"));
                organization.add(r.getInt("PersonWorkAtOrganisation.workFrom"));
                organization.add(r.getString("Place.name"));
                organizations.add(organization);
            }
        }
        return organizations;
    }

}
