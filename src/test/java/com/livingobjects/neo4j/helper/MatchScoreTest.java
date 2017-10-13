package com.livingobjects.neo4j.helper;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.Ignore;
import org.junit.Test;

import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

@Ignore
public class MatchScoreTest {

    @Test
    public void should_sort_one_match_score() throws Exception {
        PlanetByContext.MatchScore score0 = new PlanetByContext.MatchScore(0, 0, 0, 0);

        TreeSet<PlanetByContext.MatchScore> tested = Sets.newTreeSet();
        tested.addAll(ImmutableSet.of(
                score0
        ));
        assertThat(tested).containsExactly(
                score0
        );
    }

    @Test
    public void should_sort_between_two_match_scores() throws Exception {
        PlanetByContext.MatchScore score0 = new PlanetByContext.MatchScore(0, 0, 0, 0);
        PlanetByContext.MatchScore score1 = new PlanetByContext.MatchScore(1, 0, 0, 0);

        TreeSet<PlanetByContext.MatchScore> tested = Sets.newTreeSet();
        tested.addAll(ImmutableSet.of(
                score0, score1
        ));
        assertThat(tested).containsExactly(
                score1,
                score0
        );
    }

    @Test
    public void should_sort_between_three_match_scores() throws Exception {
        PlanetByContext.MatchScore score0 = new PlanetByContext.MatchScore(0, 0, 0, 0);
        PlanetByContext.MatchScore score1 = new PlanetByContext.MatchScore(1, 0, 0, 0);
        PlanetByContext.MatchScore score2 = new PlanetByContext.MatchScore(1, 1, 0, 0);

        TreeSet<PlanetByContext.MatchScore> tested = Sets.newTreeSet();
        tested.addAll(ImmutableSet.of(
                score1, score2, score0
        ));
        assertThat(tested).containsExactly(
                score2, score0, score2, score1
        );
    }

}