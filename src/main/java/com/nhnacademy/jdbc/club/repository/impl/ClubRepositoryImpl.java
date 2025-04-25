package com.nhnacademy.jdbc.club.repository.impl;

import com.nhnacademy.jdbc.club.domain.Club;
import com.nhnacademy.jdbc.club.repository.ClubRepository;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

@Slf4j
public class ClubRepositoryImpl implements ClubRepository {

    @Override
    public Optional<Club> findByClubId(Connection connection, String clubId) {
        //todo#3 club 조회
        String sql = "SELECT club_id,club_name,club_created_at FROM jdbc_club WHERE club_id = ?";
        log.debug("sql: {}", sql);

        ResultSet rs = null;
        try(PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, clubId);
            rs = preparedStatement.executeQuery();
            if(rs.next()) {
                Club newClub = new Club(
                        rs.getString("club_id"),
                        rs.getString("club_name"),
                        rs.getTimestamp("club_created_at").toLocalDateTime()
                );
                return Optional.of(newClub);
            }
        }catch(SQLException e){
            throw new RuntimeException(e);
        }finally {
            try{
                rs.close();
            }catch(SQLException e){
                throw new RuntimeException(e);
            }
        }
        return Optional.empty();
    }

    @Override
    public int save(Connection connection, Club club) {
        //todo#4 club 생성, executeUpdate() 결과를 반환
        String sql = "INSERT INTO jdbc_club SET club_id=?, club_name=? ";

        try(PreparedStatement psmt = connection.prepareStatement(sql)){
            psmt.setString(1,club.getClubId());
            psmt.setString(2,club.getClubName());
            return psmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int update(Connection connection, Club club) {
        //todo#5 club 수정, clubName을 수정합니다. executeUpdate()결과를 반환
        String sql = "UPDATE jdbc_club SET  club_name=? WHERE club_id=?";

        try(PreparedStatement psmt = connection.prepareStatement(sql)){
            psmt.setString(1,club.getClubName());
            psmt.setString(2,club.getClubId());
            return psmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int deleteByClubId(Connection connection, String clubId) {
        //todo#6 club 삭제, executeUpdate()결과 반환
        String sql = "DELETE FROM jdbc_club WHERE club_id=?";

        try(PreparedStatement psmt = connection.prepareStatement(sql)){
            psmt.setString(1,clubId);
            return psmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int countByClubId(Connection connection, String clubId) {
        //todo#7 clubId에 해당하는 club의 count를 반환
        String sql = "SELECT count(*) FROM jdbc_club WHERE club_id=?";
        ResultSet rs = null;

        try(PreparedStatement psmt = connection.prepareStatement(sql)){
            psmt.setString(1,clubId);
            rs = psmt.executeQuery();
            if(rs.next()){
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }finally {
            try {
                rs.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return 0;
    }
}
