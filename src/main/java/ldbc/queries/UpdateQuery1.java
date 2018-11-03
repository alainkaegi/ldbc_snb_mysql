/*
 * Copyright © 2018 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * The UpdateQuery1 class defines the MySQL-based update query 1.
 */
public class UpdateQuery1 {

    /**
     * Add a person.
     * @param db          A database handle
     * @param parameters  A person's full description
     * @throws SQLException if a database access error occurs
     */
    public static void query(Connection db, LdbcUpdate1AddPerson parameters) throws SQLException {

        try {
            db.setAutoCommit(false);

            PreparedStatement s;

            String addPersonQuery =
                "   INSERT INTO Person " +
                "        VALUES (?, " + // id
                "                ?, " + // firstName
                "                ?, " + // lastName
                "                ?, " + // gender
                "                ?, " + // birthday
                "                ?, " + // creationDate
                "                ?, " + // locationIP
                "                ?)";   // browserUsed
            s = db.prepareStatement(addPersonQuery);
            s.setLong(1, parameters.personId());
            s.setString(2, parameters.personFirstName());
            s.setString(3, parameters.personLastName());
            s.setString(4, parameters.gender());
            s.setLong(5, parameters.birthday().getTime());
            s.setLong(6, parameters.creationDate().getTime());
            s.setString(7, parameters.locationIp());
            s.setString(8, parameters.browserUsed());
            s.executeUpdate();

            String addLanguageLinkQuery =
                "   INSERT INTO PersonSpeaksLanguage " +
                "        VALUES (?, " + // personId
                "                ?)";   // language
            s = db.prepareStatement(addLanguageLinkQuery);
            s.setLong(1, parameters.personId());
            for (String language : parameters.languages()) {
                s.setString(2, language);
                s.executeUpdate();
            }

            String addEmailLinkQuery =
                "   INSERT INTO PersonEmailEmailAddress " +
                "        VALUES (?, " + // personId
                "                ?)";   // email
            s = db.prepareStatement(addEmailLinkQuery);
            s.setLong(1, parameters.personId());
            for (String email : parameters.emails()) {
                s.setString(2, email);
                s.executeUpdate();
            }

            String addCityLinkQuery =
                "   INSERT INTO PersonIsLocatedInPlace " +
                "        VALUES (?, " + // personId
                "                ?)";   // cityId
            s = db.prepareStatement(addCityLinkQuery);
            s.setLong(1, parameters.personId());
            s.setLong(2, parameters.cityId());
            s.executeUpdate();

            String addStudyLinkQuery =
                "   INSERT INTO PersonStudyAtOrganisation " +
                "        VALUES (?, " + // personId
                "                ?, " + // organizationId
                "                ?)";   // classYear
            s = db.prepareStatement(addStudyLinkQuery);
            s.setLong(1, parameters.personId());
            for (LdbcUpdate1AddPerson.Organization school : parameters.studyAt()) {
                s.setLong(2, school.organizationId());
                s.setInt(3, school.year());
                s.executeUpdate();
            }

            String addWorkLinkQuery =
                "   INSERT INTO PersonWorkAtOrganisation " +
                "        VALUES (?, " + // personId
                "                ?, " + // organizationId
                "                ?)"; // workFrom
            s = db.prepareStatement(addWorkLinkQuery);
            s.setLong(1, parameters.personId());
            for (LdbcUpdate1AddPerson.Organization company : parameters.workAt()) {
                s.setLong(2 , company.organizationId());
                s.setInt(3, company.year());
                s.executeUpdate();
            }

            s.close();

            db.commit();
        } finally {
            db.setAutoCommit(true);
        }
    }

}
