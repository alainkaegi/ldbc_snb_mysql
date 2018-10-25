/*
 * Copyright © 2018 Alain Kägi
 */

package ldbc.helpers;

import java.util.List;

/**
 * The Query1SortResult class defines a structure to hold one query 1
 * result temporarily.
 *
 * <p>We accumulate query 1 results temporarily in a priority queue
 * derived from this class.  We use a priority queue so we can easily
 * identify unneeded elements.
 */
public class Query1SortResult implements Comparable<Query1SortResult> {
    private final long friendId;
    private final String friendLastName;
    private final int friendDistanceFromPerson;
    private final long friendBirthday;
    private final long friendCreationDate;
    private final String friendGender;
    private final String friendBrowserUsed;
    private final String friendLocationIP;
    private final List<String> friendEmails;
    private final List<String> friendLanguages;
    private final String friendPlace;
    private final List<List<Object>> friendSchools;
    private final List<List<Object>> friendOrganizations;

    /**
     * Construct a Query1SortResult object.
     * @param friendId                  The friend's unique identifier
     * @param friendLastName            Their last name
     * @param friendDistanceFromPerson  Their distance from some start person
     * @param friendBirthday            Their birthday (in milliseconds since the start of the epoch)
     * @param friendCreationDate        Their record creation date (in milliseconds since the start of the epoch)
     * @param friendGender              Their gender
     * @param friendBrowserUsed         The browser they used
     * @param friendLocationIP          Their IP address
     * @param friendEmails              A list of their email addresses
     * @param friendLanguages           A list of the languages they speak
     * @param friendPlace               Their location
     * @param friendSchools             A list of the schools they have attended
     * @param friendOrganizations       A list of the organizations to which they have belonged
     */
    public Query1SortResult(long friendId, String friendLastName, int friendDistanceFromPerson,
                            long friendBirthday, long friendCreationDate, String friendGender,
                            String friendBrowserUsed, String friendLocationIP,
                            List<String> friendEmails, List<String> friendLanguages,
                            String friendPlace, List<List<Object>> friendSchools,
                            List<List<Object>> friendOrganizations) {
        this.friendId = friendId;
        this.friendLastName = friendLastName;
        this.friendDistanceFromPerson = friendDistanceFromPerson;
        this.friendBirthday = friendBirthday;
        this.friendCreationDate = friendCreationDate;
        this.friendGender = friendGender;
        this.friendBrowserUsed = friendBrowserUsed;
        this.friendLocationIP = friendLocationIP;
        this.friendEmails = friendEmails;
        this.friendLanguages = friendLanguages;
        this.friendPlace = friendPlace;
        this.friendSchools = friendSchools;
        this.friendOrganizations = friendOrganizations;
    }

    /**
     * Return the friend's identifier.
     * @return the friend's unique identifier
     */
    public long friendId() { return friendId; }

    /**
     * Return the friend's last name.
     * @return the friend's last name
     */
    public String friendLastName() { return friendLastName; }

    /**
     * Return the friend's distance from some person.
     * @return the friend's distance from some start person
     */
    public int friendDistanceFromPerson() { return friendDistanceFromPerson; }

    /**
     * Return the friend's birthday.
     * @return the friend's birthday (in milliseconds since the start of the epoch)
     */
    public long friendBirthday() { return friendBirthday; }

    /**
     * Return when the friend's record was created.
     * @return when the friend's record was created (in milliseconds since the start of the epoch)
     */
    public long friendCreationDate() { return friendCreationDate; }

    /**
     * Return the friend's gender.
     * @return the friend's gender
     */
    public String friendGender() { return friendGender; }

    /**
     * Return the browser used by this friend.
     * @return the browser used by this friend
     */
    public String friendBrowserUsed() { return friendBrowserUsed; }

    /**
     * Return the friend's IP address.
     * @return the friend's IP address
     */
    public String friendLocationIP() { return friendLocationIP; }

    /**
     * Return the friend's emails.
     * @return a list of this friend's emails
     */
    public List<String> friendEmails() { return friendEmails; }

    /**
     * Return the friend's spoken languages.
     * @return a list of this friend's spoken languages
     */
    public List<String> friendLanguages() { return friendLanguages; }

    /**
     * Return the friend's location.
     * @return the friend's location
     */
    public String friendPlace() { return friendPlace; }

    /**
     * Return the schools attended by this friend.
     * @return a list of the schools attended by this friend
     */
    public List<List<Object>> friendSchools() { return friendSchools; }

    /**
     * Return the organizations to which this friend has belonged.
     * @return a list of the organizations to which this friend has belonged
     */
    public List<List<Object>> friendOrganizations() { return friendOrganizations; }

    /**
     * Define a sort order for this class.
     *
     * <p>It is exactly the reverse of the intended order so we can
     * identify an unwanted element by its 'high' priority.
     *
     * @param r  result to be compared
     * @return a negative integer, zero, or a positive integer as this object is less than, equal to, or greater than the specified object
     */
    public int compareTo(Query1SortResult r) {
        int d1 = this.friendDistanceFromPerson;
        int d2 = r.friendDistanceFromPerson();
        if (d1 == d2) {
            String n1 = this.friendLastName;
            String n2 = r.friendLastName();
            if (n1.compareToIgnoreCase(n2) == 0) {
                Long id1 = this.friendId;
                Long id2 = r.friendId();
                return id2.compareTo(id1);
            }
            else
                return n2.compareToIgnoreCase(n1);
        }
        else
            return Integer.compare(d2, d1);
    }
}
