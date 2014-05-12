/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data.oozie;

import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.action.hadoop.LauncherURIHandler;
import org.apache.oozie.dependency.URIHandler;
import org.apache.oozie.dependency.URIHandlerException;
import org.apache.oozie.service.URIHandlerService;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.Datasets;

public class KiteURIHandler implements URIHandler {
  private Set<String> supportedSchemes;
  private List<Class<?>> classesToShip;

  @Override
  public void init(final Configuration conf) {
    supportedSchemes = new HashSet<String>();
    // Could split different storage schemes into different URIHandler classes
    // if desired
    final String[] schemes = conf.getStrings(
        URIHandlerService.URI_HANDLER_SUPPORTED_SCHEMES_PREFIX
            + this.getClass().getSimpleName()
            + URIHandlerService.URI_HANDLER_SUPPORTED_SCHEMES_SUFFIX, "repo");
    supportedSchemes.addAll(Arrays.asList(schemes));
    classesToShip = new KiteLauncherURIHandler().getClassesForLauncher();
  }

  @Override
  public Set<String> getSupportedSchemes() {
    return supportedSchemes;
  }

  @Override
  public Class<? extends LauncherURIHandler> getLauncherURIHandlerClass() {
    return KiteLauncherURIHandler.class;
  }

  @Override
  public List<Class<?>> getClassesForLauncher() {
    return classesToShip;
  }

  @Override
  public DependencyType getDependencyType(final URI uri)
      throws URIHandlerException {
    // TODO support PUSH with HCat repo URIs
    return DependencyType.PULL;
  }

  @Override
  public void registerForNotification(final URI uri, final Configuration conf,
      final String user, final String actionID) throws URIHandlerException {
    throw new UnsupportedOperationException(
        "Notifications are not supported for " + uri.getScheme());
  }

  @Override
  public boolean unregisterFromNotification(final URI uri, final String actionID) {
    throw new UnsupportedOperationException(
        "Notifications are not supported for " + uri.getScheme());
  }

  @Override
  public Context getContext(final URI uri, final Configuration conf,
      final String user) throws URIHandlerException {
    return null;
  }

  @Override
  public boolean exists(final URI uri, final Context context)
      throws URIHandlerException {
    final Dataset<?> dataset = Datasets.load(uri);
    return dataset != null && dataset.isFrozen();
  }

  @Override
  public boolean exists(final URI uri, final Configuration conf,
      final String user) throws URIHandlerException {
    // TODO How do we really do this honoring the user parameter? HCatURIHandler
    // doesn't really support this right now.
    final Dataset<?> dataset = Datasets.load(uri);
    return dataset != null && dataset.isFrozen();
  }

  @Override
  public String getURIWithDoneFlag(final String uri, final String doneFlag)
      throws URIHandlerException {
    return uri;
  }

  @Override
  public void validate(final String uri) throws URIHandlerException {
    // TODO add Kite API to allow URI validation
  }

  @Override
  public void destroy() {

  }

}
