package tslic.discogs;

import static tslic.discogs.Tables.ARTISTS;
import static tslic.discogs.Tables.ARTIST_ALIASES;
import static tslic.discogs.Tables.ARTIST_GROUPS;
import static tslic.discogs.Tables.ARTIST_MEMBERS;
import static tslic.discogs.Tables.ARTIST_NAMEVARIATIONS;
import static tslic.discogs.Tables.ARTIST_URLS;
import static tslic.discogs.Tables.LABELS;
import static tslic.discogs.Tables.LABEL_SUBLABELS;
import static tslic.discogs.Tables.LABEL_URLS;
import static tslic.discogs.Tables.MASTERS;
import static tslic.discogs.Tables.RELEASES;
import static tslic.discogs.Tables.RELEASE_ARTIST_MAPS;
import static tslic.discogs.Tables.RELEASE_COMPANIES;
import static tslic.discogs.Tables.RELEASE_EXTRA_ARTIST_MAPS;
import static tslic.discogs.Tables.RELEASE_FORMATS;
import static tslic.discogs.Tables.RELEASE_GENRES;
import static tslic.discogs.Tables.RELEASE_LABELS;
import static tslic.discogs.Tables.RELEASE_STYLES;
import static tslic.discogs.Tables.RELEASE_VIDEOS;
import static tslic.discogs.Tables.TRACKS;
import static tslic.discogs.Tables.TRACK_ARTIST_MAPS;
import static tslic.discogs.Tables.TRACK_EXTRA_ARTIST_MAPS;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import org.jooq.DSLContext;
import org.jooq.Insert;
import org.jooq.impl.DSL;
import tslic.discogs.dump.DumpRecords;
import tslic.discogs.dump.models.Artist;
import tslic.discogs.dump.models.ArtistAlias;
import tslic.discogs.dump.models.Company;
import tslic.discogs.dump.models.Group;
import tslic.discogs.dump.models.GroupMember;
import tslic.discogs.dump.models.Label;
import tslic.discogs.dump.models.Master;
import tslic.discogs.dump.models.Release;
import tslic.discogs.dump.models.ReleaseArtist;
import tslic.discogs.dump.models.ReleaseExtraArtist;
import tslic.discogs.dump.models.ReleaseFormat;
import tslic.discogs.dump.models.ReleaseLabel;
import tslic.discogs.dump.models.ReleaseVideo;
import tslic.discogs.dump.models.SubLabel;
import tslic.discogs.dump.models.Track;
import tslic.discogs.dump.models.TrackArtist;
import tslic.discogs.dump.models.TrackExtraArtist;

public class Main {

  public static void main(String[] args) throws SQLException, IOException {
    String dbUser = System.getenv("DB_USER");
    String dbPass = System.getenv("DB_PASSWORD");
    String dbUrl = System.getenv("DB_URL");

    try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPass)) {
      DSLContext create = DSL.using(conn);

      for (String arg : args) {
        if (arg.contains("artists")) {
          try (InputStream is = openStream(arg)) {
            insertArtists(DumpRecords.readArtists(is), create);
          }
        } else if (arg.contains("releases")) {
          try (InputStream is = openStream(arg)) {
            insertReleases(DumpRecords.readReleases(is), create);
          }
        } else if (arg.contains("masters")) {
          try (InputStream is = openStream(arg)) {
            insertMasters(DumpRecords.readMasters(is), create);
          }
        } else if (arg.contains("labels")) {
          try (InputStream is = openStream(arg)) {
            insertLabels(DumpRecords.readLabels(is), create);
          }
        }
      }
    }
  }

  private static InputStream openStream(String arg) throws IOException {
    final InputStream is;

    if (arg.startsWith("http")) {
      is = new URL(arg).openStream();
    } else {
      is = new FileInputStream(arg);
    }

    return arg.endsWith(".gz") ? new GZIPInputStream(is) : is;
  }

  private static void insertArtists(Iterator<Artist> artists, DSLContext create) {
    try (InsertExecutor executor = new InsertExecutor(create)) {
      while (artists.hasNext()) {
        Artist artist = artists.next();

        executor.add(
            create
                .insertInto(ARTISTS)
                .set(ARTISTS.ID, artist.getId())
                .set(ARTISTS.NAME, artist.getName())
                .set(ARTISTS.REAL_NAME, artist.getRealName())
                .set(ARTISTS.DATA_QUALITY, artist.getDataQuality())
                .set(ARTISTS.PROFILE, artist.getProfile())
                .set(ARTISTS.STATUS, artist.getStatus()));

        int offset = 0;
        for (String nv : artist.getNameVariations()) {
          executor.add(
              create
                  .insertInto(ARTIST_NAMEVARIATIONS)
                  .set(ARTIST_NAMEVARIATIONS.ARTIST_ID, artist.getId())
                  .set(ARTIST_NAMEVARIATIONS.OFST, offset++)
                  .set(ARTIST_NAMEVARIATIONS.NAMEVARIATION, nv));
        }

        offset = 0;
        for (ArtistAlias alias : artist.getAliases()) {
          executor.add(
              create
                  .insertInto(ARTIST_ALIASES)
                  .set(ARTIST_ALIASES.ARTIST_ID, artist.getId())
                  .set(ARTIST_ALIASES.OFST, offset++)
                  .set(ARTIST_ALIASES.ARTIST2_ID, alias.getId())
                  .set(ARTIST_ALIASES.NAME, alias.getName()));
        }

        offset = 0;
        for (Group group : artist.getGroups()) {
          executor.add(
              create
                  .insertInto(ARTIST_GROUPS)
                  .set(ARTIST_GROUPS.ARTIST_ID, artist.getId())
                  .set(ARTIST_GROUPS.OFST, offset++)
                  .set(ARTIST_GROUPS.ARTIST2_ID, group.getId())
                  .set(ARTIST_GROUPS.NAME, group.getName()));
        }

        offset = 0;
        for (GroupMember groupMember : artist.getMembers()) {
          executor.add(
              create
                  .insertInto(ARTIST_MEMBERS)
                  .set(ARTIST_MEMBERS.ARTIST_ID, artist.getId())
                  .set(ARTIST_MEMBERS.OFST, offset++)
                  .set(ARTIST_MEMBERS.ARTIST2_ID, groupMember.getId())
                  .set(ARTIST_MEMBERS.NAME, groupMember.getName()));
        }

        offset = 0;
        for (String url : artist.getUrls()) {
          executor.add(
              create
                  .insertInto(ARTIST_URLS)
                  .set(ARTIST_URLS.ARTIST_ID, artist.getId())
                  .set(ARTIST_URLS.OFST, offset++)
                  .set(ARTIST_URLS.URL, url));
        }
      }
    }
  }

  private static void insertReleases(Iterator<Release> releases, DSLContext create) {
    try (InsertExecutor executor = new InsertExecutor(create)) {
      while (releases.hasNext()) {
        Release release = releases.next();

        executor.add(
            create
                .insertInto(RELEASES)
                .set(RELEASES.ID, release.getId())
                .set(RELEASES.TITLE, release.getTitle())
                .set(RELEASES.COUNTRY, release.getCountry())
                .set(RELEASES.NOTES, release.getNotes())
                .set(RELEASES.RELEASED, release.getReleased())
                .set(RELEASES.STATUS, release.getStatus())
                .set(RELEASES.DATA_QUALITY, release.getDataQuality())
                .set(RELEASES.MASTER_ID, release.getMasterId()));

        int offset = 0;
        for (String genre : release.getGenres()) {
          executor.add(
              create
                  .insertInto(RELEASE_GENRES)
                  .set(RELEASE_GENRES.RELEASE_ID, release.getId())
                  .set(RELEASE_GENRES.OFST, offset++)
                  .set(RELEASE_GENRES.GENRE, genre));
        }

        offset = 0;
        for (String style : release.getStyles()) {
          executor.add(
              create
                  .insertInto(RELEASE_STYLES)
                  .set(RELEASE_STYLES.RELEASE_ID, release.getId())
                  .set(RELEASE_STYLES.OFST, offset++)
                  .set(RELEASE_STYLES.STYLE, style));
        }

        offset = 0;
        for (ReleaseArtist artist : release.getArtists()) {
          executor.add(
              create
                  .insertInto(RELEASE_ARTIST_MAPS)
                  .set(RELEASE_ARTIST_MAPS.RELEASE_ID, release.getId())
                  .set(RELEASE_ARTIST_MAPS.OFST, offset++)
                  .set(RELEASE_ARTIST_MAPS.ARTIST_ID, artist.getId())
                  .set(RELEASE_ARTIST_MAPS.NAME, artist.getName())
                  .set(RELEASE_ARTIST_MAPS.ANV, artist.getAnv())
                  .set(RELEASE_ARTIST_MAPS.JOIN_RELATION, artist.getJoin())
                  .set(RELEASE_ARTIST_MAPS.TRACKS, artist.getTracks()));
        }

        offset = 0;
        for (ReleaseExtraArtist artist : release.getExtraArtists()) {
          executor.add(
              create
                  .insertInto(RELEASE_EXTRA_ARTIST_MAPS)
                  .set(RELEASE_EXTRA_ARTIST_MAPS.RELEASE_ID, release.getId())
                  .set(RELEASE_EXTRA_ARTIST_MAPS.OFST, offset++)
                  .set(RELEASE_EXTRA_ARTIST_MAPS.ARTIST_ID, artist.getId())
                  .set(RELEASE_EXTRA_ARTIST_MAPS.NAME, artist.getName())
                  .set(RELEASE_EXTRA_ARTIST_MAPS.ANV, artist.getAnv())
                  .set(RELEASE_EXTRA_ARTIST_MAPS.JOIN_RELATION, artist.getJoin())
                  .set(RELEASE_EXTRA_ARTIST_MAPS.TRACKS, artist.getTracks())
                  .set(RELEASE_EXTRA_ARTIST_MAPS.ROLE, artist.getRole()));
        }

        offset = 0;
        for (ReleaseFormat format : release.getFormats()) {
          executor.add(
              create
                  .insertInto(RELEASE_FORMATS)
                  .set(RELEASE_FORMATS.RELEASE_ID, release.getId())
                  .set(RELEASE_FORMATS.OFST, offset++)
                  .set(RELEASE_FORMATS.NAME, format.getName())
                  .set(RELEASE_FORMATS.QTY, format.getQty())
                  .set(RELEASE_FORMATS.TEXT, format.getText()));
        }

        offset = 0;
        for (ReleaseVideo video : release.getVideos()) {
          executor.add(
              create
                  .insertInto(RELEASE_VIDEOS)
                  .set(RELEASE_VIDEOS.RELEASE_ID, release.getId())
                  .set(RELEASE_VIDEOS.OFST, offset++)
                  .set(RELEASE_VIDEOS.SRC, video.getSrc())
                  .set(RELEASE_VIDEOS.DURATION, video.getDuration())
                  .set(RELEASE_VIDEOS.EMBED, video.getEmbed())
                  .set(RELEASE_VIDEOS.TITLE, video.getTitle())
                  .set(RELEASE_VIDEOS.DESCRIPTION, video.getDescription()));
        }

        offset = 0;
        for (Company company : release.getCompanies()) {
          executor.add(
              create
                  .insertInto(RELEASE_COMPANIES)
                  .set(RELEASE_COMPANIES.RELEASE_ID, release.getId())
                  .set(RELEASE_COMPANIES.OFST, offset++)
                  .set(RELEASE_COMPANIES.COMPANY_ID, company.getId())
                  .set(RELEASE_COMPANIES.NAME, company.getName())
                  .set(RELEASE_COMPANIES.CATNO, company.getCatno())
                  .set(RELEASE_COMPANIES.ENTITY_TYPE, company.getEntityType())
                  .set(RELEASE_COMPANIES.ENTITY_TYPE_NAME, company.getEntityTypeName()));
        }

        offset = 0;
        for (ReleaseLabel label : release.getLabels()) {
          executor.add(
              create
                  .insertInto(RELEASE_LABELS)
                  .set(RELEASE_LABELS.RELEASE_ID, release.getId())
                  .set(RELEASE_LABELS.OFST, offset++)
                  .set(RELEASE_LABELS.LABEL_ID, label.getId())
                  .set(RELEASE_LABELS.NAME, label.getName())
                  .set(RELEASE_LABELS.CATNO, label.getCatno()));
        }

        int trackOffset = 0;
        for (Track track : release.getTracks()) {
          executor.add(
              create
                  .insertInto(TRACKS)
                  .set(TRACKS.RELEASE_ID, release.getId())
                  .set(TRACKS.OFST, trackOffset)
                  .set(TRACKS.TITLE, track.getTitle())
                  .set(TRACKS.DURATION, track.getDuration())
                  .set(TRACKS.POSITION, track.getPosition()));

          offset = 0;
          for (TrackArtist artist : track.getArtists()) {
            executor.add(
                create
                    .insertInto(TRACK_ARTIST_MAPS)
                    .set(TRACK_ARTIST_MAPS.RELEASE_ID, release.getId())
                    .set(TRACK_ARTIST_MAPS.TRACK_OFST, trackOffset)
                    .set(TRACK_ARTIST_MAPS.ARTIST_OFST, offset++)
                    .set(TRACK_ARTIST_MAPS.ARTIST_ID, artist.getId())
                    .set(TRACK_ARTIST_MAPS.NAME, artist.getName())
                    .set(TRACK_ARTIST_MAPS.ANV, artist.getAnv())
                    .set(TRACK_ARTIST_MAPS.JOIN_RELATION, artist.getJoin()));
          }

          offset = 0;
          for (TrackExtraArtist artist : track.getExtraArtists()) {
            executor.add(
                create
                    .insertInto(TRACK_EXTRA_ARTIST_MAPS)
                    .set(TRACK_EXTRA_ARTIST_MAPS.RELEASE_ID, release.getId())
                    .set(TRACK_EXTRA_ARTIST_MAPS.TRACK_OFST, trackOffset)
                    .set(TRACK_EXTRA_ARTIST_MAPS.ARTIST_OFST, offset++)
                    .set(TRACK_EXTRA_ARTIST_MAPS.ARTIST_ID, artist.getId())
                    .set(TRACK_EXTRA_ARTIST_MAPS.NAME, artist.getName())
                    .set(TRACK_EXTRA_ARTIST_MAPS.ANV, artist.getAnv())
                    .set(TRACK_EXTRA_ARTIST_MAPS.JOIN_RELATION, artist.getJoin())
                    .set(TRACK_EXTRA_ARTIST_MAPS.ROLE, artist.getRole()));
          }

          trackOffset++;
        }
      }
    }
  }

  private static void insertMasters(Iterator<Master> masters, DSLContext create) {
    try (InsertExecutor executor = new InsertExecutor(create)) {
      while (masters.hasNext()) {
        Master master = masters.next();

        executor.add(
            create
                .insertInto(MASTERS)
                .set(MASTERS.ID, master.getId())
                .set(MASTERS.MAIN_RELEASE_ID, master.getMainReleaseId()));
      }
    }
  }

  private static void insertLabels(Iterator<Label> labels, DSLContext create) {
    try (InsertExecutor executor = new InsertExecutor(create)) {
      while (labels.hasNext()) {
        Label label = labels.next();

        executor.add(
            create
                .insertInto(LABELS)
                .set(LABELS.ID, label.getId())
                .set(LABELS.NAME, label.getName())
                .set(LABELS.CONTACT_INFO, label.getContactInfo())
                .set(LABELS.PROFILE, label.getProfile())
                .set(LABELS.DATA_QUALITY, label.getDataQuality()));

        int offset = 0;
        for (SubLabel subLabel : label.getSubLabels()) {
          executor.add(
              create
                  .insertInto(LABEL_SUBLABELS)
                  .set(LABEL_SUBLABELS.LABEL_ID, label.getId())
                  .set(LABEL_SUBLABELS.OFST, offset++)
                  .set(LABEL_SUBLABELS.LABEL2_ID, subLabel.getId())
                  .set(LABEL_SUBLABELS.NAME, subLabel.getName()));
        }

        offset = 0;
        for (String url : label.getUrls()) {
          executor.add(
              create
                  .insertInto(LABEL_URLS)
                  .set(LABEL_URLS.LABEL_ID, label.getId())
                  .set(LABEL_URLS.OFST, offset++)
                  .set(LABEL_URLS.URL, url));
        }
      }
    }
  }

  static class InsertExecutor implements AutoCloseable {

    private static final int BATCH_SIZE = 100;

    private final DSLContext create;
    private final List<Insert> insertions = new LinkedList<>();

    InsertExecutor(DSLContext create) {
      this.create = create;
    }

    void add(Insert insert) {
      insertions.add(insert);

      if (insertions.size() == BATCH_SIZE) {
        create.batch(insertions).execute();
        insertions.clear();
      }
    }

    private void executeBatch() {
      create.batch(insertions).execute();
    }

    @Override
    public void close() {
      executeBatch();
    }
  }
}
