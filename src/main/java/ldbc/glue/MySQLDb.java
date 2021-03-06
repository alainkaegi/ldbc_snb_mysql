/*
 * Copyright © 2018-2019 Alain Kägi
 */

package ldbc.glue;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14Result;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfileResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPosts;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPostsResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriends;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriendsResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContent;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContentResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreator;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreatorResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForumResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageRepliesResult;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate2AddPostLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate3AddCommentLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate5AddForumMembership;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate6AddPost;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate7AddComment;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate8AddFriendship;

import com.zaxxer.hikari.HikariDataSource;

import java.io.IOException;

import java.sql.SQLException;

import java.util.Map;
import java.util.List;

import ldbc.queries.*;

/**
 * The MySQLDB class interfaces between the LDBC driver and this
 * MySQL-based LDBC SNB implementation.
 */
public class MySQLDb extends Db {

    private MySQLDbConnectionState state = null;

    @Override
    protected void onInit(Map<String, String> properties) throws DbException {
        /* Register complex read queries. */
        registerOperationHandler(LdbcQuery1.class, Query1Handler.class);
        registerOperationHandler(LdbcQuery2.class, Query2Handler.class);
        registerOperationHandler(LdbcQuery3.class, Query3Handler.class);
        registerOperationHandler(LdbcQuery4.class, Query4Handler.class);
        registerOperationHandler(LdbcQuery5.class, Query5Handler.class);
        registerOperationHandler(LdbcQuery6.class, Query6Handler.class);
        registerOperationHandler(LdbcQuery7.class, Query7Handler.class);
        registerOperationHandler(LdbcQuery8.class, Query8Handler.class);
        registerOperationHandler(LdbcQuery9.class, Query9Handler.class);
        registerOperationHandler(LdbcQuery10.class, Query10Handler.class);
        registerOperationHandler(LdbcQuery11.class, Query11Handler.class);
        registerOperationHandler(LdbcQuery12.class, Query12Handler.class);
        registerOperationHandler(LdbcQuery13.class, Query13Handler.class);
        registerOperationHandler(LdbcQuery14.class, Query14Handler.class);

        /* Register short read queries. */
        registerOperationHandler(LdbcShortQuery1PersonProfile.class, ShortQuery1Handler.class);
        registerOperationHandler(LdbcShortQuery2PersonPosts.class, ShortQuery2Handler.class);
        registerOperationHandler(LdbcShortQuery3PersonFriends.class, ShortQuery3Handler.class);
        registerOperationHandler(LdbcShortQuery4MessageContent.class, ShortQuery4Handler.class);
        registerOperationHandler(LdbcShortQuery5MessageCreator.class, ShortQuery5Handler.class);
        registerOperationHandler(LdbcShortQuery6MessageForum.class, ShortQuery6Handler.class);
        registerOperationHandler(LdbcShortQuery7MessageReplies.class, ShortQuery7Handler.class);

        /* Register update queries. */
        registerOperationHandler(LdbcUpdate1AddPerson.class, UpdateQuery1Handler.class);
        registerOperationHandler(LdbcUpdate2AddPostLike.class, UpdateQuery2Handler.class);
        registerOperationHandler(LdbcUpdate3AddCommentLike.class, UpdateQuery3Handler.class);
        registerOperationHandler(LdbcUpdate4AddForum.class, UpdateQuery4Handler.class);
        registerOperationHandler(LdbcUpdate5AddForumMembership.class, UpdateQuery5Handler.class);
        registerOperationHandler(LdbcUpdate6AddPost.class, UpdateQuery6Handler.class);
        registerOperationHandler(LdbcUpdate7AddComment.class, UpdateQuery7Handler.class);
        registerOperationHandler(LdbcUpdate8AddFriendship.class, UpdateQuery8Handler.class);

        state = new MySQLDbConnectionState(properties.get("url"), properties.get("user"), properties.get("password"));
    }

    @Override
    protected void onClose() throws IOException {}

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return state;
    }


    /* The complex read queries connectors. */

    public static class Query1Handler implements OperationHandler<LdbcQuery1, MySQLDbConnectionState> {
        @Override
        public void executeOperation(LdbcQuery1 operation, MySQLDbConnectionState state, ResultReporter result) throws DbException {
            try {
                HikariDataSource client = state.getClient();
                List<LdbcQuery1Result> r = Query1.query(client, operation.personId(), operation.firstName(), operation.limit());
                result.report(r.size(), r, operation);
            }
            catch (SQLException e) {
                throw new DbException(e.getMessage());
            }
        }
    }

    public static class Query2Handler implements OperationHandler<LdbcQuery2, MySQLDbConnectionState> {
        @Override
        public void executeOperation(LdbcQuery2 operation, MySQLDbConnectionState state, ResultReporter result) throws DbException {
            try {
                HikariDataSource client = state.getClient();
                List<LdbcQuery2Result> r = Query2.query(client, operation.personId(), operation.maxDate().getTime(), operation.limit());
                result.report(r.size(), r, operation);
            }
            catch (SQLException e) {
                throw new DbException(e.getMessage());
            }
        }
    }

    public static class Query3Handler implements OperationHandler<LdbcQuery3, MySQLDbConnectionState> {
        @Override
        public void executeOperation(LdbcQuery3 operation, MySQLDbConnectionState state, ResultReporter result) throws DbException {
            try {
                HikariDataSource client = state.getClient();
                List<LdbcQuery3Result> r = Query3.query(client, operation.personId(), operation.countryXName(), operation.countryYName(), operation.startDate().getTime(), operation.durationDays(), operation.limit());
                result.report(r.size(), r, operation);
            }
            catch (SQLException e) {
                throw new DbException(e.getMessage());
            }
        }
    }

    public static class Query4Handler implements OperationHandler<LdbcQuery4, MySQLDbConnectionState> {
        @Override
        public void executeOperation(LdbcQuery4 operation, MySQLDbConnectionState state, ResultReporter result) throws DbException {
            try {
                HikariDataSource client = state.getClient();
                List<LdbcQuery4Result> r = Query4.query(client, operation.personId(), operation.startDate().getTime(), operation.durationDays(), operation.limit());
                result.report(r.size(), r, operation);
            }
            catch (SQLException e) {
                throw new DbException(e.getMessage());
            }
        }
    }

    public static class Query5Handler implements OperationHandler<LdbcQuery5, MySQLDbConnectionState> {
        @Override
        public void executeOperation(LdbcQuery5 operation, MySQLDbConnectionState state, ResultReporter result) throws DbException {
            try {
                HikariDataSource client = state.getClient();
                List<LdbcQuery5Result> r = Query5.query(client, operation.personId(), operation.minDate().getTime(), operation.limit());
                result.report(r.size(), r, operation);
            }
            catch (SQLException e) {
                throw new DbException(e.getMessage());
            }
        }
    }

    public static class Query6Handler implements OperationHandler<LdbcQuery6, MySQLDbConnectionState> {
        @Override
        public void executeOperation(LdbcQuery6 operation, MySQLDbConnectionState state, ResultReporter result) throws DbException {
            try {
                HikariDataSource client = state.getClient();
                List<LdbcQuery6Result> r = Query6.query(client, operation.personId(), operation.tagName(), operation.limit());
                result.report(r.size(), r, operation);
            }
            catch (SQLException e) {
                throw new DbException(e.getMessage());
            }
        }
    }

    public static class Query7Handler implements OperationHandler<LdbcQuery7, MySQLDbConnectionState> {
        @Override
        public void executeOperation(LdbcQuery7 operation, MySQLDbConnectionState state, ResultReporter result) throws DbException {
            try {
                HikariDataSource client = state.getClient();
                List<LdbcQuery7Result> r = Query7.query(client, operation.personId(), operation.limit());
                result.report(r.size(), r, operation);
            }
            catch (SQLException e) {
                throw new DbException(e.getMessage());
            }
        }
    }

    public static class Query8Handler implements OperationHandler<LdbcQuery8, MySQLDbConnectionState> {
        @Override
        public void executeOperation(LdbcQuery8 operation, MySQLDbConnectionState state, ResultReporter result) throws DbException {
            try {
                HikariDataSource client = state.getClient();
                List<LdbcQuery8Result> r = Query8.query(client, operation.personId(), operation.limit());
                result.report(r.size(), r, operation);
            }
            catch (SQLException e) {
                throw new DbException(e.getMessage());
            }
        }
    }

    public static class Query9Handler implements OperationHandler<LdbcQuery9, MySQLDbConnectionState> {
        @Override
        public void executeOperation(LdbcQuery9 operation, MySQLDbConnectionState state, ResultReporter result) throws DbException {
            try {
                HikariDataSource client = state.getClient();
                List<LdbcQuery9Result> r = Query9.query(client, operation.personId(), operation.maxDate().getTime(), operation.limit());
                result.report(r.size(), r, operation);
            }
            catch (SQLException e) {
                throw new DbException(e.getMessage());
            }
        }
    }

    public static class Query10Handler implements OperationHandler<LdbcQuery10, MySQLDbConnectionState> {
        @Override
        public void executeOperation(LdbcQuery10 operation, MySQLDbConnectionState state, ResultReporter result) throws DbException {
            try {
                HikariDataSource client = state.getClient();
                List<LdbcQuery10Result> r = Query10.query(client, operation.personId(), operation.month(), operation.limit());
                result.report(r.size(), r, operation);
            }
            catch (SQLException e) {
                throw new DbException(e.getMessage());
            }
        }
    }

    public static class Query11Handler implements OperationHandler<LdbcQuery11, MySQLDbConnectionState> {
        @Override
        public void executeOperation(LdbcQuery11 operation, MySQLDbConnectionState state, ResultReporter result) throws DbException {
            try {
                HikariDataSource client = state.getClient();
                List<LdbcQuery11Result> r = Query11.query(client, operation.personId(), operation.countryName(), operation.workFromYear(), operation.limit());
                result.report(r.size(), r, operation);
            }
            catch (SQLException e) {
                throw new DbException(e.getMessage());
            }
        }
    }

    public static class Query12Handler implements OperationHandler<LdbcQuery12, MySQLDbConnectionState> {
        @Override
        public void executeOperation(LdbcQuery12 operation, MySQLDbConnectionState state, ResultReporter result) throws DbException {
            try {
                HikariDataSource client = state.getClient();
                List<LdbcQuery12Result> r = Query12.query(client, operation.personId(), operation.tagClassName(), operation.limit());
                result.report(r.size(), r, operation);
            }
            catch (SQLException e) {
                throw new DbException(e.getMessage());
            }
        }
    }

    public static class Query13Handler implements OperationHandler<LdbcQuery13, MySQLDbConnectionState> {
        @Override
        public void executeOperation(LdbcQuery13 operation, MySQLDbConnectionState state, ResultReporter result) throws DbException {
            try {
                HikariDataSource client = state.getClient();
                LdbcQuery13Result r = Query13.query(client, operation.person1Id(), operation.person2Id());
                result.report(1, r, operation);
            }
            catch (SQLException e) {
                throw new DbException(e.getMessage());
            }
        }
    }

    public static class Query14Handler implements OperationHandler<LdbcQuery14, MySQLDbConnectionState> {
        @Override
        public void executeOperation(LdbcQuery14 operation, MySQLDbConnectionState state, ResultReporter result) throws DbException {
            try {
                HikariDataSource client = state.getClient();
                List<LdbcQuery14Result> r = Query14.query(client, operation.person1Id(), operation.person2Id());
                result.report(r.size(), r, operation);
            }
            catch (SQLException e) {
                throw new DbException(e.getMessage());
            }
        }
    }


    /* The short read queries connectors. */

    public static class ShortQuery1Handler implements OperationHandler<LdbcShortQuery1PersonProfile, MySQLDbConnectionState> {
        @Override
        public void executeOperation(LdbcShortQuery1PersonProfile operation, MySQLDbConnectionState state, ResultReporter result) throws DbException {
            try {
                HikariDataSource client = state.getClient();
                LdbcShortQuery1PersonProfileResult r = ShortQuery1.query(client, operation.personId());
                result.report(1, r, operation);
            }
            catch (SQLException e) {
                throw new DbException(e.getMessage());
            }
        }
    }

    public static class ShortQuery2Handler implements OperationHandler<LdbcShortQuery2PersonPosts, MySQLDbConnectionState> {
        @Override
        public void executeOperation(LdbcShortQuery2PersonPosts operation, MySQLDbConnectionState state, ResultReporter result) throws DbException {
            try {
                HikariDataSource client = state.getClient();
                List<LdbcShortQuery2PersonPostsResult> r = ShortQuery2.query(client, operation.personId(), operation.limit());
                result.report(r.size(), r, operation);
            }
            catch (SQLException e) {
                throw new DbException(e.getMessage());
            }
        }
    }

    public static class ShortQuery3Handler implements OperationHandler<LdbcShortQuery3PersonFriends, MySQLDbConnectionState> {
        @Override
        public void executeOperation(LdbcShortQuery3PersonFriends operation, MySQLDbConnectionState state, ResultReporter result) throws DbException {
            try {
                HikariDataSource client = state.getClient();
                List<LdbcShortQuery3PersonFriendsResult> r = ShortQuery3.query(client, operation.personId());
                result.report(r.size(), r, operation);
            }
            catch (SQLException e) {
                throw new DbException(e.getMessage());
            }
        }
    }

    public static class ShortQuery4Handler implements OperationHandler<LdbcShortQuery4MessageContent, MySQLDbConnectionState> {
        @Override
        public void executeOperation(LdbcShortQuery4MessageContent operation, MySQLDbConnectionState state, ResultReporter result) throws DbException {
            try {
                HikariDataSource client = state.getClient();
                LdbcShortQuery4MessageContentResult r = ShortQuery4.query(client, operation.messageId());
                result.report(1, r, operation);
            }
            catch (SQLException e) {
                throw new DbException(e.getMessage());
            }
        }
    }

    public static class ShortQuery5Handler implements OperationHandler<LdbcShortQuery5MessageCreator, MySQLDbConnectionState> {
        @Override
        public void executeOperation(LdbcShortQuery5MessageCreator operation, MySQLDbConnectionState state, ResultReporter result) throws DbException {
            try {
                HikariDataSource client = state.getClient();
                LdbcShortQuery5MessageCreatorResult r = ShortQuery5.query(client, operation.messageId());
                result.report(1, r, operation);
            }
            catch (SQLException e) {
                throw new DbException(e.getMessage());
            }
        }
    }

    public static class ShortQuery6Handler implements OperationHandler<LdbcShortQuery6MessageForum, MySQLDbConnectionState> {
        @Override
        public void executeOperation(LdbcShortQuery6MessageForum operation, MySQLDbConnectionState state, ResultReporter result) throws DbException {
            try {
                HikariDataSource client = state.getClient();
                LdbcShortQuery6MessageForumResult r = ShortQuery6.query(client, operation.messageId());
                result.report(1, r, operation);
            }
            catch (SQLException e) {
                throw new DbException(e.getMessage());
            }
        }
    }

    public static class ShortQuery7Handler implements OperationHandler<LdbcShortQuery7MessageReplies, MySQLDbConnectionState> {
        @Override
        public void executeOperation(LdbcShortQuery7MessageReplies operation, MySQLDbConnectionState state, ResultReporter result) throws DbException {
            try {
                HikariDataSource client = state.getClient();
                List<LdbcShortQuery7MessageRepliesResult> r = ShortQuery7.query(client, operation.messageId());
                result.report(r.size(), r, operation);
            }
            catch (SQLException e) {
                throw new DbException(e.getMessage());
            }
        }
    }


    /* The update queries connectors. */

    public static class UpdateQuery1Handler implements OperationHandler<LdbcUpdate1AddPerson, MySQLDbConnectionState> {
        @Override
        public void executeOperation(LdbcUpdate1AddPerson operation, MySQLDbConnectionState state, ResultReporter result) throws DbException {
            try {
                HikariDataSource client = state.getClient();
                UpdateQuery1.query(client, operation);
                result.report(0, LdbcNoResult.INSTANCE, operation);
            }
            catch (SQLException e) {
                throw new DbException(e.getMessage());
            }
        }
    }

    public static class UpdateQuery2Handler implements OperationHandler<LdbcUpdate2AddPostLike, MySQLDbConnectionState> {
        @Override
        public void executeOperation(LdbcUpdate2AddPostLike operation, MySQLDbConnectionState state, ResultReporter result) throws DbException {
            try {
                HikariDataSource client = state.getClient();
                UpdateQuery2.query(client, operation);
                result.report(0, LdbcNoResult.INSTANCE, operation);
            }
            catch (SQLException e) {
                throw new DbException(e.getMessage());
            }
        }
    }

    public static class UpdateQuery3Handler implements OperationHandler<LdbcUpdate3AddCommentLike, MySQLDbConnectionState> {
        @Override
        public void executeOperation(LdbcUpdate3AddCommentLike operation, MySQLDbConnectionState state, ResultReporter result) throws DbException {
            try {
                HikariDataSource client = state.getClient();
                UpdateQuery3.query(client, operation);
                result.report(0, LdbcNoResult.INSTANCE, operation);
            }
            catch (SQLException e) {
                throw new DbException(e.getMessage());
            }
        }
    }

    public static class UpdateQuery4Handler implements OperationHandler<LdbcUpdate4AddForum, MySQLDbConnectionState> {
        @Override
        public void executeOperation(LdbcUpdate4AddForum operation, MySQLDbConnectionState state, ResultReporter result) throws DbException {
            try {
                HikariDataSource client = state.getClient();
                UpdateQuery4.query(client, operation);
                result.report(0, LdbcNoResult.INSTANCE, operation);
            }
            catch (SQLException e) {
                throw new DbException(e.getMessage());
            }
        }
    }

    public static class UpdateQuery5Handler implements OperationHandler<LdbcUpdate5AddForumMembership, MySQLDbConnectionState> {
        @Override
        public void executeOperation(LdbcUpdate5AddForumMembership operation, MySQLDbConnectionState state, ResultReporter result) throws DbException {
            try {
                HikariDataSource client = state.getClient();
                UpdateQuery5.query(client, operation);
                result.report(0, LdbcNoResult.INSTANCE, operation);
            }
            catch (SQLException e) {
                throw new DbException(e.getMessage());
            }
        }
    }

    public static class UpdateQuery6Handler implements OperationHandler<LdbcUpdate6AddPost, MySQLDbConnectionState> {
        @Override
        public void executeOperation(LdbcUpdate6AddPost operation, MySQLDbConnectionState state, ResultReporter result) throws DbException {
            try {
                HikariDataSource client = state.getClient();
                UpdateQuery6.query(client, operation);
                result.report(0, LdbcNoResult.INSTANCE, operation);
            }
            catch (SQLException e) {
                throw new DbException(e.getMessage());
            }
        }
    }

    public static class UpdateQuery7Handler implements OperationHandler<LdbcUpdate7AddComment, MySQLDbConnectionState> {
        @Override
        public void executeOperation(LdbcUpdate7AddComment operation, MySQLDbConnectionState state, ResultReporter result) throws DbException {
            try {
                HikariDataSource client = state.getClient();
                UpdateQuery7.query(client, operation);
                result.report(0, LdbcNoResult.INSTANCE, operation);
            }
            catch (SQLException e) {
                throw new DbException(e.getMessage());
            }
        }
    }

    public static class UpdateQuery8Handler implements OperationHandler<LdbcUpdate8AddFriendship, MySQLDbConnectionState> {
        @Override
        public void executeOperation(LdbcUpdate8AddFriendship operation, MySQLDbConnectionState state, ResultReporter result) throws DbException {
            try {
                HikariDataSource client = state.getClient();
                UpdateQuery8.query(client, operation);
                result.report(0, LdbcNoResult.INSTANCE, operation);
            }
            catch (SQLException e) {
                throw new DbException(e.getMessage());
            }
        }
    }

}
