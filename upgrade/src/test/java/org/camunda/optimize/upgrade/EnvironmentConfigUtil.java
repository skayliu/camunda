package org.camunda.optimize.upgrade;

import org.camunda.optimize.upgrade.service.ValidationService;

import java.io.File;
import java.io.FileWriter;
import java.net.URISyntaxException;

public class EnvironmentConfigUtil {

  private EnvironmentConfigUtil() {
    super();
  }

  public static void createEmptyEnvConfig() throws Exception {
    createEnvConfig("");
  }

  public static void createEnvConfig(String content) throws Exception {
    File env = getClasspathFolder();
    File config = new File(env.getAbsolutePath() + "/environment-config.yaml");

    if (!config.exists()) {
      config.createNewFile();
      FileWriter fileWriter = new FileWriter(config);
      if (content != null && !content.isEmpty()) {
        fileWriter.append(content);
      } else {
        fileWriter.write("");
      }
      fileWriter.close();
    }
  }

  public static void deleteEnvConfig() throws Exception {
    File env = getClasspathFolder();
    File config = new File(env.getAbsolutePath() + "/environment-config.yaml");

    if (config.exists()) {
      config.delete();
    }
  }

  public static File getClasspathFolder() throws URISyntaxException {
    String executionFolderPath = ValidationService.class.
      getProtectionDomain()
      .getCodeSource()
      .getLocation()
      .toURI()
      .getPath();
    return new File(executionFolderPath);
  }
}