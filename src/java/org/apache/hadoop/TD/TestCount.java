package org.apache.hadoop.TD;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.PrintStream;
import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.Count;
import org.apache.hadoop.fs.shell.PathData;
import org.junit.Test;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * JUnit test class for {@link org.apache.hadoop.fs.shell.Count}
 * 
 */
public class TestCount {
  private static final String WITH_QUOTAS = "Content summary with quotas";
  private static final String NO_QUOTAS = "Content summary without quotas";
  private static final String HUMAN = "human: ";
  private static final String BYTES = "bytes: ";
  private static Configuration conf;
  private static FileSystem mockFs;
  private static FileStatus fileStat;
  private static ContentSummary mockCs;

  @BeforeClass
  public static void setup() {
    conf = new Configuration();
    conf.setClass("fs.mockfs.impl", MockFileSystem.class, FileSystem.class);
    mockFs = mock(FileSystem.class);
    fileStat = mock(FileStatus.class);
    mockCs = mock(ContentSummary.class);
    when(fileStat.isFile()).thenReturn(true);
  }

  @Before
  public void resetMock() {
    reset(mockFs);
  }

  @Test
  public void processOptionsHumanReadable() {
    LinkedList<String> options = new LinkedList<String>();
    options.add("-h");
    options.add("dummy");
    Count count = new Count();
    count.processOptions(options);
    assertFalse(count.isShowQuotas());
    assertTrue(count.isHumanReadable());
  }

  @Test
  public void processOptionsAll() {
    LinkedList<String> options = new LinkedList<String>();
    options.add("-q");
    options.add("-h");
    options.add("dummy");
    Count count = new Count();
    count.processOptions(options);
    assertTrue(count.isShowQuotas());
    assertTrue(count.isHumanReadable());
  }

  // check quotas are reported correctly
  @Test
  public void processPathShowQuotas() throws Exception {
    Path path = new Path("mockfs:/test");

    when(mockFs.getFileStatus(eq(path))).thenReturn(fileStat);
    PathData pathData = new PathData(path.toString(), conf);

    PrintStream out = mock(PrintStream.class);

    Count count = new Count();
    count.out = out;

    LinkedList<String> options = new LinkedList<String>();
    options.add("-q");
    options.add("dummy");
    count.processOptions(options);

    count.processPath(pathData);
    verify(out).println(BYTES + WITH_QUOTAS + path.toString());
    verifyNoMoreInteractions(out);
  }

  // check counts without quotas are reported correctly
  @Test
  public void processPathNoQuotas() throws Exception {
    Path path = new Path("mockfs:/test");

    when(mockFs.getFileStatus(eq(path))).thenReturn(fileStat);
    PathData pathData = new PathData(path.toString(), conf);

    PrintStream out = mock(PrintStream.class);

    Count count = new Count();
    count.out = out;

    LinkedList<String> options = new LinkedList<String>();
    options.add("dummy");
    count.processOptions(options);

    count.processPath(pathData);
    verify(out).println(BYTES + NO_QUOTAS + path.toString());
    verifyNoMoreInteractions(out);
  }

  @Test
  public void processPathShowQuotasHuman() throws Exception {
    Path path = new Path("mockfs:/test");

    when(mockFs.getFileStatus(eq(path))).thenReturn(fileStat);
    PathData pathData = new PathData(path.toString(), conf);

    PrintStream out = mock(PrintStream.class);

    Count count = new Count();
    count.out = out;

    LinkedList<String> options = new LinkedList<String>();
    options.add("-q");
    options.add("-h");
    options.add("dummy");
    count.processOptions(options);

    count.processPath(pathData);
    verify(out).println(HUMAN + WITH_QUOTAS + path.toString());
  }

  @Test
  public void processPathNoQuotasHuman() throws Exception {
    Path path = new Path("mockfs:/test");

    when(mockFs.getFileStatus(eq(path))).thenReturn(fileStat);
    PathData pathData = new PathData(path.toString(), conf);

    PrintStream out = mock(PrintStream.class);

    Count count = new Count();
    count.out = out;

    LinkedList<String> options = new LinkedList<String>();
    options.add("-h");
    options.add("dummy");
    count.processOptions(options);

    count.processPath(pathData);
    verify(out).println(HUMAN + NO_QUOTAS + path.toString());
  }

  @Test
  public void getCommandName() {
    Count count = new Count();
    String actual = count.getCommandName();
    String expected = "count";
    assertEquals("Count.getCommandName", expected, actual);
  }

  @Test
  public void isDeprecated() {
    Count count = new Count();
    boolean actual = count.isDeprecated();
    boolean expected = false;
    assertEquals("Count.isDeprecated", expected, actual);
  }

  @Test
  public void getReplacementCommand() {
    Count count = new Count();
    String actual = count.getReplacementCommand();
    String expected = null;
    assertEquals("Count.getReplacementCommand", expected, actual);
  }

  @Test
  public void getName() {
    Count count = new Count();
    String actual = count.getName();
    String expected = "count";
    assertEquals("Count.getName", expected, actual);
  }

  @Test
  public void getUsage() {
    Count count = new Count();
    String actual = count.getUsage();
    String expected = "-count [-q] [-h] <path> ...";
    assertEquals("Count.getUsage", expected, actual);
  }


  // mock content system
  static class MockContentSummary extends ContentSummary {
    
    public MockContentSummary() {}

    @Override
    public String toString(boolean qOption, boolean hOption) {
      if (qOption) {
        if (hOption) {
          return(HUMAN + WITH_QUOTAS);
        } else {
          return(BYTES + WITH_QUOTAS);
        }
      } else {
        if (hOption) {
          return(HUMAN + NO_QUOTAS);
        } else {
          return(BYTES + NO_QUOTAS);
        }
      }
    }
  }

  // mock file system for use in testing
  static class MockFileSystem extends FilterFileSystem {
    Configuration conf;

    MockFileSystem() {
      super(mockFs);
    }

    @Override
    public void initialize(URI uri, Configuration conf) {
      this.conf = conf;
    }

    @Override
    public Path makeQualified(Path path) {
      return path;
    }

    @Override
    public ContentSummary getContentSummary(Path f) throws IOException {
      return new MockContentSummary();
    }

    @Override
    public Configuration getConf() {
      return conf;
    }
  }
}
