/*
 * Copyright 2021 OPPO. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.oppo.shuttle.rss;

import com.oppo.shuttle.rss.common.MasterDispatchServers;
import com.oppo.shuttle.rss.common.Ors2WorkerDetail;
import com.oppo.shuttle.rss.common.ServerDetailWithStatus;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class ShuffleWorkerDistributeTest {

    @Test
    public void simpleTest() {
        ArrayList<ServerDetailWithStatus> s = new ArrayList<>();
        s.add(new ServerDetailWithStatus(new Ors2WorkerDetail("c1", 1, 1), 1));
        s.add(new ServerDetailWithStatus(new Ors2WorkerDetail("c2", 1, 1), 1));
        s.add(new ServerDetailWithStatus(new Ors2WorkerDetail("c3", 1, 1), 2));
        s.add(new ServerDetailWithStatus(new Ors2WorkerDetail("c4", 1, 1), 2));

        MasterDispatchServers dispatch = new MasterDispatchServers("dc", "c", "", s, "");

        List<Ors2WorkerDetail> list = dispatch.getCandidatesByWeight(6);

        assert list.size() == 6;
        assert list.stream().filter(x -> x.getHost().equals("c1")).count() == 1;
        assert list.stream().filter(x -> x.getHost().equals("c2")).count() == 1;
        assert list.stream().filter(x -> x.getHost().equals("c3")).count() == 2;
        assert list.stream().filter(x -> x.getHost().equals("c4")).count() == 2;


        list = dispatch.getCandidatesByWeight(2);
        assert list.size() == 2;

        list = dispatch.getCandidatesByWeight(1000);
        assert list.size() == 6;
        assert list.stream().filter(x -> x.getHost().equals("c1")).count() == 1;
        assert list.stream().filter(x -> x.getHost().equals("c2")).count() == 1;
        assert list.stream().filter(x -> x.getHost().equals("c3")).count() == 2;
        assert list.stream().filter(x -> x.getHost().equals("c4")).count() == 2;
    }


    @Test
    public void bigWeight() {
        ArrayList<ServerDetailWithStatus> s = new ArrayList<>();
        s.add(new ServerDetailWithStatus(new Ors2WorkerDetail("c1", 1, 1), 100));
        s.add(new ServerDetailWithStatus(new Ors2WorkerDetail("c2", 1, 1), 100));
        s.add(new ServerDetailWithStatus(new Ors2WorkerDetail("c3", 1, 1), 200));
        s.add(new ServerDetailWithStatus(new Ors2WorkerDetail("c4", 1, 1), 200));

        MasterDispatchServers dispatch = new MasterDispatchServers("dc", "c", "", s, "");

        List<Ors2WorkerDetail> list = dispatch.getCandidatesByWeight(6);

        assert list.size() == 6;
        assert list.stream().filter(x -> x.getHost().equals("c1")).count() == 1;
        assert list.stream().filter(x -> x.getHost().equals("c2")).count() == 1;
        assert list.stream().filter(x -> x.getHost().equals("c3")).count() == 2;
        assert list.stream().filter(x -> x.getHost().equals("c4")).count() == 2;


        list = dispatch.getCandidatesByWeight(2);
        assert list.size() == 2;

        list = dispatch.getCandidatesByWeight(1000);
        assert list.size() == 6;
        assert list.stream().filter(x -> x.getHost().equals("c1")).count() == 1;
        assert list.stream().filter(x -> x.getHost().equals("c2")).count() == 1;
        assert list.stream().filter(x -> x.getHost().equals("c3")).count() == 2;
        assert list.stream().filter(x -> x.getHost().equals("c4")).count() == 2;
    }


    @Test
    public void probability1() {
        ArrayList<ServerDetailWithStatus> s = new ArrayList<>();
        s.add(new ServerDetailWithStatus(new Ors2WorkerDetail("c1", 1, 1), 1));
        s.add(new ServerDetailWithStatus(new Ors2WorkerDetail("c2", 1, 1), 1));
        s.add(new ServerDetailWithStatus(new Ors2WorkerDetail("c3", 1, 1), 2));
        s.add(new ServerDetailWithStatus(new Ors2WorkerDetail("c4", 1, 1), 2));

        MasterDispatchServers dispatch = new MasterDispatchServers("dc", "c", "", s, "");

        int c1 = 0;
        int c2 = 0;
        int c3 = 0;
        int c4 = 0;

        long start = System.currentTimeMillis();
        for (int i = 0; i <= 100000; i++) {
            List<Ors2WorkerDetail> list = dispatch.getCandidatesByWeight(4);
            c1 += list.stream().filter(x -> x.getHost().equals("c1")).count();
            c2 += list.stream().filter(x -> x.getHost().equals("c2")).count();
            c3 += list.stream().filter(x -> x.getHost().equals("c3")).count();
            c4 += list.stream().filter(x -> x.getHost().equals("c4")).count();
        }
        long cost = System.currentTimeMillis() - start;

        double sum = c1 + c2 + c3 + c4;
        assert ((int)(c1 / sum * 100) == 16);
        assert ((int)(c2 / sum * 100) == 16);
        assert ((int)(c3 / sum * 100) == 33);
        assert ((int)(c3 / sum * 100) == 33);

        System.out.println("cost: " + cost + " ms");
    }


    @Test
    public void probability2() {
        ArrayList<ServerDetailWithStatus> s = new ArrayList<>();
        s.add(new ServerDetailWithStatus(new Ors2WorkerDetail("c1", 1, 1), 1));
        s.add(new ServerDetailWithStatus(new Ors2WorkerDetail("c2", 1, 1), 1));
        s.add(new ServerDetailWithStatus(new Ors2WorkerDetail("c3", 1, 1), 2));
        s.add(new ServerDetailWithStatus(new Ors2WorkerDetail("c4", 1, 1), 2));

        MasterDispatchServers dispatch = new MasterDispatchServers("dc", "c", "", s, "");

        int c1 = 0;
        int c2 = 0;
        int c3 = 0;
        int c4 = 0;

        long start = System.currentTimeMillis();
        for (int i = 0; i <= 100000; i++) {
            List<Ors2WorkerDetail> list = dispatch.getCandidatesByWeight(1000);
            assert list.size() == 6;
            c1 += list.stream().filter(x -> x.getHost().equals("c1")).count();
            c2 += list.stream().filter(x -> x.getHost().equals("c2")).count();
            c3 += list.stream().filter(x -> x.getHost().equals("c3")).count();
            c4 += list.stream().filter(x -> x.getHost().equals("c4")).count();
        }
        long cost = System.currentTimeMillis() - start;

        double sum = c1 + c2 + c3 + c4;
        assert ((int)(c1 / sum * 100) == 16);
        assert ((int)(c2 / sum * 100) == 16);
        assert ((int)(c3 / sum * 100) == 33);
        assert ((int)(c3 / sum * 100) == 33);

        System.out.println("cost: " + cost + " ms");
    }
}
