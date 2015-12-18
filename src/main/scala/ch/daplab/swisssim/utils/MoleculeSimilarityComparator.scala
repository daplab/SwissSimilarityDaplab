// Copyright (C) 2015 the original author or authors.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ch.daplab.swisssim.utils

import java.util.Comparator

/**
  * Created by bperroud on 12/17/15.
  */
class MoleculeSimilarityComparator extends Comparator[(Double, Array[Byte])] {
  override def compare(o1: (Double, Array[Byte]), o2: (Double, Array[Byte])): Int = {
    -o1._1.compareTo(o2._1)
  }
}
