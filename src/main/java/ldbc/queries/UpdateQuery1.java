/*
 * Copyright © 2018-2019 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * The UpdateQuery1 class defines the MySQL-based update query 1.
 */
public class UpdateQuery1 {

    /**
     * Add a person.
     * @param ds          A data source
     * @param parameters  A person's full description
     * @throws SQLException if a database access error occurs
     */
    public static void query(HikariDataSource ds, LdbcUpdate1AddPerson parameters) throws SQLException {

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

        String addLanguageLinkQuery =
            "   INSERT INTO PersonSpeaksLanguage " +
            "        VALUES (?, " + // personId
            "                ?)";   // language

        String addEmailLinkQuery =
            "   INSERT INTO PersonEmailEmailAddress " +
            "        VALUES (?, " + // personId
            "                ?)";   // email

        String addCityLinkQuery =
            "   INSERT INTO PersonIsLocatedInPlace " +
            "        VALUES (?, " + // personId
            "                ?)";   // cityId

        String addStudyLinkQuery =
            "   INSERT INTO PersonStudyAtOrganisation " +
            "        VALUES (?, " + // personId
            "                ?, " + // organizationId
            "                ?)";   // classYear

        String addWorkLinkQuery =
            "   INSERT INTO PersonWorkAtOrganisation " +
            "        VALUES (?, " + // personId
            "                ?, " + // organizationId
            "                ?)"; // workFrom

        try (Connection c = ds.getConnection();
             PreparedStatement addPersonStatement = c.prepareStatement(addPersonQuery);
             PreparedStatement addLanguageLinkStatement = c.prepareStatement(addLanguageLinkQuery);
             PreparedStatement addEmailLinkStatement = c.prepareStatement(addEmailLinkQuery);
             PreparedStatement addCityLinkStatement = c.prepareStatement(addCityLinkQuery);
             PreparedStatement addStudyLinkStatement = c.prepareStatement(addStudyLinkQuery);
             PreparedStatement addWorkLinkStatement = c.prepareStatement(addWorkLinkQuery)) {
            addPersonStatement.setLong(1, parameters.personId());
            addPersonStatement.setString(2, parameters.personFirstName());
            addPersonStatement.setString(3, parameters.personLastName());
            addPersonStatement.setString(4, parameters.gender());
            addPersonStatement.setLong(5, parameters.birthday().getTime());
            addPersonStatement.setLong(6, parameters.creationDate().getTime());
            addPersonStatement.setString(7, parameters.locationIp());
            addPersonStatement.setString(8, parameters.browserUsed());
            addPersonStatement.executeUpdate();

            addLanguageLinkStatement.setLong(1, parameters.personId());
            for (String language : parameters.languages()) {
                addLanguageLinkStatement.setString(2, language);
                addLanguageLinkStatement.executeUpdate();
            }

            addEmailLinkStatement.setLong(1, parameters.personId());
            for (String email : parameters.emails()) {
                addEmailLinkStatement.setString(2, email);
                addEmailLinkStatement.executeUpdate();
            }

            addCityLinkStatement.setLong(1, parameters.personId());
            addCityLinkStatement.setLong(2, parameters.cityId());
            addCityLinkStatement.executeUpdate();

            addStudyLinkStatement.setLong(1, parameters.personId());
            for (LdbcUpdate1AddPerson.Organization school : parameters.studyAt()) {
                addStudyLinkStatement.setLong(2, school.organizationId());
                addStudyLinkStatement.setInt(3, school.year());
                addStudyLinkStatement.executeUpdate();
            }

            addWorkLinkStatement.setLong(1, parameters.personId());
            for (LdbcUpdate1AddPerson.Organization company : parameters.workAt()) {
                addWorkLinkStatement.setLong(2 , company.organizationId());
                addWorkLinkStatement.setInt(3, company.year());
                addWorkLinkStatement.executeUpdate();
            }

            c.commit();
        }
    }

}
