package com.nhnacademy.jdbc.student.repository.impl;

import com.nhnacademy.jdbc.student.domain.Student;
import com.nhnacademy.jdbc.student.repository.StudentRepository;
import com.nhnacademy.jdbc.util.DbUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

@Slf4j
public class StatementStudentRepository implements StudentRepository {

    @Override
    public int save(Student student){
        //todo#1 insert student
        String query = String.format("INSERT INTO jdbc_students(id,name,gender,age) values('%s', '%s', '%s', %d)",
                student.getId(),
                student.getName(),
                student.getGender(),
                student.getAge()
        );
        log.debug("Insert query: {}", query);

        try(Connection conn = DbUtils.getConnection();
            Statement statement = conn.createStatement();) {
            int result = statement.executeUpdate(query);
            log.debug("Insert result: {}", result);
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<Student> findById(String id){
        //todo#2 student 조회
        String query = String.format("SELECT * FROM jdbc_students WHERE id = '%s'", id);
        log.debug("Select query: {}", query);

        try(Connection conn = DbUtils.getConnection();
            Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(query)) {
            if(resultSet.next()) {
                Student student = new Student(resultSet.getString("id"),
                        resultSet.getString("name"),
                        Student.GENDER.valueOf(resultSet.getString("gender")),
                        resultSet.getInt("age"),
                        resultSet.getTimestamp("created_at").toLocalDateTime()
                );
                return Optional.of(student);
            }
        }catch(SQLException e) {
            throw new RuntimeException(e);
        }
        return Optional.empty();
    }

    @Override
    public int update(Student student){
        //todo#3 student 수정, name <- 수정합니다.
        String query = String.format("UPDATE jdbc_students SET name='%s', gender='%s', age='%d' WHERE id = '%s'",
                student.getName(),
                student.getGender(),
                student.getAge(),
                student.getId()
        );
        log.debug("Update query: {}", query);

        try(Connection conn = DbUtils.getConnection();
            Statement statement = conn.createStatement();) {
            int result = statement.executeUpdate(query);
            log.debug("Update result: {}", result);
            return result;
        }catch(SQLException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public int deleteById(String id){
       //todo#4 student 삭제
        String query = String.format("DELETE FROM jdbc_students WHERE id = '%s'", id);
        log.debug("Delete query: {}", query);

        try(Connection conn = DbUtils.getConnection();
        Statement statement = conn.createStatement();) {
            int result = statement.executeUpdate(query);
            log.debug("Delete result: {}", result);
            return result;
        }catch(SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
