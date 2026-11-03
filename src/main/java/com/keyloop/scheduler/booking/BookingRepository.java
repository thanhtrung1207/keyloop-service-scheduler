package com.keyloop.scheduler.booking;

import com.keyloop.scheduler.catalog.Dealership;
import com.keyloop.scheduler.catalog.ServiceType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZoneId;
import java.time.LocalTime;
import java.util.Optional;
import java.util.UUID;

@Repository
public class BookingRepository {
    public record AvailableBay(UUID id, String bayCode) { }
    public record AvailableTechnician(UUID id, String fullName) { }

    private final JdbcClient jdbc;

    public BookingRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<Dealership> findDealership(UUID dealershipId) {
        return jdbc.sql("""
                select id, name, timezone, open_time, close_time
                from dealerships
                where id = :dealershipId
                """)
                .param("dealershipId", dealershipId)
                .query((rs, row) -> new Dealership(
                        rs.getObject("id", UUID.class),
                        rs.getString("name"),
                        ZoneId.of(rs.getString("timezone")),
                        rs.getObject("open_time", LocalTime.class),
                        rs.getObject("close_time", LocalTime.class)))
                .optional();
    }

    public Optional<ServiceType> findServiceType(UUID serviceTypeId) {
        return jdbc.sql("""
                select id, code, name, duration_minutes, required_skill
                from service_types
                where id = :serviceTypeId
                """)
                .param("serviceTypeId", serviceTypeId)
                .query((rs, row) -> new ServiceType(
                        rs.getObject("id", UUID.class),
                        rs.getString("code"),
                        rs.getString("name"),
                        rs.getInt("duration_minutes"),
                        rs.getString("required_skill")))
                .optional();
    }

    public void lockDealership(UUID dealershipId) {
        jdbc.sql("""
                select 1
                from (select pg_advisory_xact_lock(hashtextextended(cast(:dealershipId as text), 0))) lock_result
                """)
                .param("dealershipId", dealershipId)
                .query(Integer.class)
                .single();
    }

    public boolean hasVinOverlap(String vin, TimeSlot slot) {
        Integer count = jdbc.sql("""
                select count(*)
                from appointments
                where vehicle_vin = :vin
                  and start_time < :slotEnd
                  and end_time > :slotStart
                """)
                .param("vin", vin)
                .param("slotStart", databaseTimestamp(slot.start()))
                .param("slotEnd", databaseTimestamp(slot.end()))
                .query(Integer.class)
                .single();
        return count > 0;
    }

    public Optional<AvailableBay> findFreeBay(UUID dealershipId, TimeSlot slot) {
        return jdbc.sql("""
                select b.id, b.bay_code
                from service_bays b
                where b.dealership_id = :dealershipId
                  and b.status = 'ACTIVE'
                  and not exists (
                      select 1
                      from appointments a
                      where a.service_bay_id = b.id
                        and a.start_time < :slotEnd
                        and a.end_time > :slotStart
                  )
                order by b.bay_code, b.id
                limit 1
                """)
                .param("dealershipId", dealershipId)
                .param("slotStart", databaseTimestamp(slot.start()))
                .param("slotEnd", databaseTimestamp(slot.end()))
                .query((rs, row) -> new AvailableBay(
                        rs.getObject("id", UUID.class), rs.getString("bay_code")))
                .optional();
    }

    public Optional<AvailableTechnician> findFreeTechnician(UUID dealershipId, String skillCode, TimeSlot slot) {
        return jdbc.sql("""
                select t.id, t.full_name
                from technicians t
                join technician_skills required_skill
                  on required_skill.technician_id = t.id
                 and required_skill.skill_code = :skillCode
                where t.dealership_id = :dealershipId
                  and t.status = 'ACTIVE'
                  and not exists (
                      select 1
                      from appointments a
                      where a.technician_id = t.id
                        and a.start_time < :slotEnd
                        and a.end_time > :slotStart
                  )
                order by (
                    select count(*)
                    from technician_skills all_skills
                    where all_skills.technician_id = t.id
                ), t.full_name, t.id
                limit 1
                """)
                .param("dealershipId", dealershipId)
                .param("skillCode", skillCode)
                .param("slotStart", databaseTimestamp(slot.start()))
                .param("slotEnd", databaseTimestamp(slot.end()))
                .query((rs, row) -> new AvailableTechnician(
                        rs.getObject("id", UUID.class), rs.getString("full_name")))
                .optional();
    }

    private OffsetDateTime databaseTimestamp(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC);
    }
}
