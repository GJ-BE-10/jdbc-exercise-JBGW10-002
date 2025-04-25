package com.nhnacademy.jdbc.user.repository.impl;

import com.nhnacademy.jdbc.user.domain.User;
import com.nhnacademy.jdbc.user.repository.UserRepository;
import com.nhnacademy.jdbc.util.DbUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.Optional;

@Slf4j
public class PreparedStatementUserRepository implements UserRepository {
    @Override
    public Optional<User> findByUserIdAndUserPassword(String userId, String userPassword) {
        //todo#11 -PreparedStatement- 아이디 , 비밀번호가 일치하는 회원조회
        String sql = "SELECT * FROM jdbc_users WHERE user_id=? AND user_password=?";
        log.debug("SQL: {}", sql);

        ResultSet resultSet = null;
        try(Connection connection = DbUtils.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql);) {
            preparedStatement.setString(1, userId);
            preparedStatement.setString(2, userPassword);
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                User user = new User(
                        resultSet.getString("user_id"),
                        resultSet.getString("user_name"),
                        resultSet.getString("user_password")
                );
                return Optional.of(user);
            }
        }catch(SQLException e){
            throw new RuntimeException(e);
        }finally {
            try{
                resultSet.close();
            }catch (SQLException e){
                throw new RuntimeException(e);
            }
        }
        return Optional.empty();
    }

    @Override
    public Optional<User> findById(String userId) {
        //todo#12-PreparedStatement-회원조회
        String sql = "SELECT * FROM jdbc_users WHERE user_id=?";
        log.debug("SQL: {}", sql);

        ResultSet resultSet = null;
        try(Connection connection = DbUtils.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);) {
            preparedStatement.setString(1, userId);
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                User user = new User(
                        resultSet.getString("user_id"),
                        resultSet.getString("user_name"),
                        resultSet.getString("user_password")
                );
                return Optional.of(user);
            }
        }catch(SQLException e){
            throw new RuntimeException(e);
        }finally {
            try{
                resultSet.close();
            }catch (SQLException e){
                throw new RuntimeException(e);
            }
        }
        return Optional.empty();
    }

    @Override
    public int save(User user) {
        //todo#13-PreparedStatement-회원저장
        String sql = "INSERT INTO jdbc_users(user_id, user_name, user_password) VALUES (?,?,?)";
        log.debug("SQL: {}", sql);

        try(Connection connection = DbUtils.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);) {
            preparedStatement.setString(1, user.getUserId());
            preparedStatement.setString(2, user.getUserName());
            preparedStatement.setString(3, user.getUserPassword());
            int result = preparedStatement.executeUpdate();
            log.debug("SQL: {}", result);

            return result;
        }catch(SQLException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public int updateUserPasswordByUserId(String userId, String userPassword) {
        //todo#14-PreparedStatement-회원정보 수정
        String sql = "UPDATE jdbc_users SET user_password=? WHERE user_id=?";
        log.debug("SQL: {}", sql);

        try(Connection connection = DbUtils.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);) {
            preparedStatement.setString(1, userPassword);
            preparedStatement.setString(2, userId);

            int result = preparedStatement.executeUpdate();
            log.debug("SQL: {}", result);

            return result;
        }catch(SQLException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public int deleteByUserId(String userId) {
        //todo#15-PreparedStatement-회원삭제
        String sql = "DELETE FROM jdbc_users WHERE user_id=?";
        log.debug("SQL: {}", sql);

        try(Connection connection = DbUtils.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);) {
            preparedStatement.setString(1, userId);

            int result = preparedStatement.executeUpdate();
            log.debug("SQL: {}", result);

            return result;
        }catch(SQLException e){
            throw new RuntimeException(e);
        }
    }
}
