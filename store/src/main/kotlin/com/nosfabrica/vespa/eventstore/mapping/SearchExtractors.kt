/*
 * Copyright (c) 2026 NosFabrica
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the
 * Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN
 * AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package com.nosfabrica.vespa.eventstore.mapping

import com.nosfabrica.vespa.eventstore.engine.doc.SearchFields
import com.vitorpamplona.quartz.buzz.agentProfiles.AgentProfileEvent
import com.vitorpamplona.quartz.buzz.apPersonas.PersonaEvent
import com.vitorpamplona.quartz.buzz.managedAgents.ManagedAgentEvent
import com.vitorpamplona.quartz.buzz.teams.TeamEvent
import com.vitorpamplona.quartz.buzz.workflow.WorkflowDefEvent
import com.vitorpamplona.quartz.experimental.agora.FundraiserEvent
import com.vitorpamplona.quartz.experimental.audio.track.AudioTrackEvent
import com.vitorpamplona.quartz.experimental.fitness.workout.ExerciseTemplateEvent
import com.vitorpamplona.quartz.experimental.fitness.workout.WorkoutRecordEvent
import com.vitorpamplona.quartz.experimental.interactiveStories.InteractiveStoryBaseEvent
import com.vitorpamplona.quartz.experimental.music.playlist.MusicPlaylistEvent
import com.vitorpamplona.quartz.experimental.music.track.MusicTrackEvent
import com.vitorpamplona.quartz.experimental.nip82SoftwareApps.application.SoftwareApplicationEvent
import com.vitorpamplona.quartz.experimental.nipsOnNostr.NipTextEvent
import com.vitorpamplona.quartz.feedDefinition.FeedDefinitionEvent
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.tags.hashtags.hashtags
import com.vitorpamplona.quartz.nip10Notes.TextNoteEvent
import com.vitorpamplona.quartz.nip14Subject.subject
import com.vitorpamplona.quartz.nip23LongContent.LongTextNoteEvent
import com.vitorpamplona.quartz.nip28PublicChat.admin.ChannelCreateEvent
import com.vitorpamplona.quartz.nip28PublicChat.admin.ChannelMetadataEvent
import com.vitorpamplona.quartz.nip29RelayGroups.metadata.GroupMetadataEvent
import com.vitorpamplona.quartz.nip30CustomEmoji.pack.EmojiPackEvent
import com.vitorpamplona.quartz.nip34Git.issue.GitIssueEvent
import com.vitorpamplona.quartz.nip34Git.pr.GitPullRequestEvent
import com.vitorpamplona.quartz.nip34Git.repository.GitRepositoryEvent
import com.vitorpamplona.quartz.nip35Torrents.TorrentEvent
import com.vitorpamplona.quartz.nip50Search.SearchableEvent
import com.vitorpamplona.quartz.nip51Lists.appCurationSet.AppCurationSetEvent
import com.vitorpamplona.quartz.nip51Lists.articleCurationSet.ArticleCurationSetEvent
import com.vitorpamplona.quartz.nip51Lists.bookmarkList.BookmarkListEvent
import com.vitorpamplona.quartz.nip51Lists.bookmarkList.OldBookmarkListEvent
import com.vitorpamplona.quartz.nip51Lists.followList.FollowListEvent
import com.vitorpamplona.quartz.nip51Lists.interestSet.InterestSetEvent
import com.vitorpamplona.quartz.nip51Lists.labeledBookmarkList.LabeledBookmarkListEvent
import com.vitorpamplona.quartz.nip51Lists.mediaStarterPack.MediaStarterPackEvent
import com.vitorpamplona.quartz.nip51Lists.peopleList.PeopleListEvent
import com.vitorpamplona.quartz.nip51Lists.pictureCurationSet.PictureCurationSetEvent
import com.vitorpamplona.quartz.nip51Lists.relaySets.RelaySetEvent
import com.vitorpamplona.quartz.nip51Lists.releaseArtifactSet.ReleaseArtifactSetEvent
import com.vitorpamplona.quartz.nip51Lists.videoCurationSet.VideoCurationSetEvent
import com.vitorpamplona.quartz.nip52Calendar.appt.day.CalendarDateSlotEvent
import com.vitorpamplona.quartz.nip52Calendar.appt.time.CalendarTimeSlotEvent
import com.vitorpamplona.quartz.nip52Calendar.calendar.CalendarEvent
import com.vitorpamplona.quartz.nip53LiveActivities.clip.LiveActivitiesClipEvent
import com.vitorpamplona.quartz.nip53LiveActivities.meetingSpaces.MeetingRoomEvent
import com.vitorpamplona.quartz.nip53LiveActivities.meetingSpaces.MeetingSpaceEvent
import com.vitorpamplona.quartz.nip53LiveActivities.streaming.LiveActivitiesEvent
import com.vitorpamplona.quartz.nip54Wiki.WikiNoteEvent
import com.vitorpamplona.quartz.nip58Badges.definition.BadgeDefinitionEvent
import com.vitorpamplona.quartz.nip5aStaticWebsites.NamedSiteEvent
import com.vitorpamplona.quartz.nip5aStaticWebsites.RootSiteEvent
import com.vitorpamplona.quartz.nip5dNapplets.NamedNappletEvent
import com.vitorpamplona.quartz.nip5dNapplets.NappletSnapshotEvent
import com.vitorpamplona.quartz.nip5dNapplets.RootNappletEvent
import com.vitorpamplona.quartz.nip68Picture.PictureEvent
import com.vitorpamplona.quartz.nip71Video.AddressableVideoEvent
import com.vitorpamplona.quartz.nip71Video.RegularVideoEvent
import com.vitorpamplona.quartz.nip72ModCommunities.definition.CommunityDefinitionEvent
import com.vitorpamplona.quartz.nip75ZapGoals.GoalEvent
import com.vitorpamplona.quartz.nip7DThreads.ThreadEvent
import com.vitorpamplona.quartz.nip84Highlights.HighlightEvent
import com.vitorpamplona.quartz.nip89AppHandlers.definition.AppDefinitionEvent
import com.vitorpamplona.quartz.nip94FileMetadata.FileHeaderEvent
import com.vitorpamplona.quartz.nip99Classifieds.ClassifiedsEvent
import com.vitorpamplona.quartz.nipB0WebBookmarks.WebBookmarkEvent
import com.vitorpamplona.quartz.nipC0CodeSnippets.CodeSnippetEvent
import com.vitorpamplona.quartz.nipF4Podcasts.episode.PodcastEpisodeEvent
import com.vitorpamplona.quartz.nipF4Podcasts.metadata.PodcastMetadataEvent

/**
 * Decomposes every Quartz [SearchableEvent] into the schema's search fields by
 * priority tier: title-like accessors primary, summary/description/hashtags
 * secondary, body tertiary. Each explicit branch splits exactly the accessors
 * that kind's `indexableContent()` concatenates — mirror Quartz changes here.
 * Kinds without an explicit branch fall back to the [SearchableEvent] branch
 * (whole `indexableContent()` in the tertiary tier), so EVERY searchable kind,
 * current or future, is imported.
 *
 * A kind may also fill the profile *role* columns when it carries that shape:
 * kind 31990 goes through the kind-0 columns wholesale, and any kind with a
 * homepage/site URL fills [SearchFields.website]. The rank profiles compose
 * role columns with `max()`/sum, so this cross-kind reuse is safe.
 *
 * Hashtags (secondary tier) and `location` tags ([SearchFields.location]) are
 * filled SYSTEMICALLY in a post-pass ([extract]), never by the branches.
 * Non-searchable kinds return [SearchFields.NONE]. Extractors are derived
 * data: changes roll out with `reindexFullTextSearch`, no resync.
 */
object SearchExtractors {
    fun extract(event: Event): SearchFields = augment(event, base(event))

    /**
     * Systemic post-pass: fold hashtags into the secondary tier and `location`
     * tags into the location column for every tier kind, so recall never
     * depends on a branch remembering them. Profile-shaped kinds (kind 0, app
     * handlers) own their columns and are left untouched.
     */
    private fun augment(
        event: Event,
        base: SearchFields,
    ): SearchFields {
        if (event !is SearchableEvent || event is MetadataEvent || event is AppDefinitionEvent) return base
        return base.copy(
            secondary = join(base.secondary, hashtags(event)),
            location = base.location ?: join(tagValues(event, "location")),
        )
    }

    private fun base(event: Event): SearchFields =
        when (event) {
            // kind 0 -> the profile fields, each in its own role.
            is MetadataEvent -> {
                val md = event.contactMetaData()
                if (md == null) {
                    SearchFields.NONE
                } else {
                    SearchFields(
                        name = clean(md.name),
                        displayName = clean(md.displayName),
                        about = clean(md.about),
                        nip05 = clean(md.nip05),
                        lud16 = clean(md.lud16),
                        website = clean(md.website),
                    )
                }
            }

            is LongTextNoteEvent -> {
                tiers(event.title(), event.summary(), event.content)
            }

            is WikiNoteEvent -> {
                tiers(event.title(), event.summary(), event.content)
            }

            is ClassifiedsEvent -> {
                tiers(event.title(), event.summary(), event.content)
            }

            is GitRepositoryEvent -> {
                tiers(event.name(), event.description(), event.content, website = join(event.webs()))
            }

            is GitIssueEvent -> {
                tiers(event.subject(), null, event.content)
            }

            is GitPullRequestEvent -> {
                tiers(event.subject(), null, event.content)
            }

            is CommunityDefinitionEvent -> {
                tiers(event.name(), join(event.description(), event.rules()), event.content)
            }

            is EmojiPackEvent -> {
                tiers(event.titleOrName(), event.description(), event.content)
            }

            is ChannelCreateEvent -> {
                event.channelInfo().let { tiers(it.name, it.about, null) }
            }

            is ChannelMetadataEvent -> {
                event.channelInfo().let { tiers(it.name, it.about, null) }
            }

            is PictureEvent -> {
                tiers(event.title(), null, event.content)
            }

            is RegularVideoEvent -> {
                tiers(event.title(), null, event.content)
            }

            is AddressableVideoEvent -> {
                tiers(event.title(), null, event.content)
            }

            // Torrents are searched by FILE NAME above all — index the file
            // list into the secondary tier, trackers as the affiliation URL.
            is TorrentEvent -> {
                tiers(event.title(), join(tagValues(event, "file")), event.content, website = join(event.trackers()))
            }

            is ThreadEvent -> {
                tiers(event.title(), null, event.content)
            }

            is FundraiserEvent -> {
                tiers(event.title(), null, event.content)
            }

            is NipTextEvent -> {
                tiers(event.title(), null, event.content)
            }

            is ExerciseTemplateEvent -> {
                tiers(event.title(), null, event.content)
            }

            is WorkoutRecordEvent -> {
                tiers(event.title(), null, event.content)
            }

            is CalendarEvent -> {
                tiers(event.title(), null, event.content)
            }

            is LiveActivitiesClipEvent -> {
                tiers(event.title(), null, event.content)
            }

            is CalendarDateSlotEvent -> {
                tiers(event.title(), event.summary(), event.content)
            }

            is CalendarTimeSlotEvent -> {
                tiers(event.title(), event.summary(), event.content)
            }

            is LiveActivitiesEvent -> {
                tiers(event.title(), event.summary(), event.content, website = event.streaming())
            }

            is InteractiveStoryBaseEvent -> {
                tiers(event.title(), event.summary(), event.content)
            }

            is MeetingSpaceEvent -> {
                tiers(event.room(), event.summary(), event.content)
            }

            is MeetingRoomEvent -> {
                tiers(event.title(), event.summary(), null)
            }

            // Code snippets are searched by language/runtime as much as name —
            // fold those keywords into the secondary tier, repo as affiliation.
            is CodeSnippetEvent -> {
                tiers(
                    event.snippetName(),
                    join(event.snippetDescription(), event.language(), event.extension(), event.runtime()),
                    event.content,
                    website = clean(event.repo()),
                )
            }

            is BadgeDefinitionEvent -> {
                tiers(event.name(), event.description(), event.content)
            }

            is MusicPlaylistEvent -> {
                tiers(event.title(), event.description(), event.content)
            }

            is MusicTrackEvent -> {
                tiers(event.title(), join(event.artist(), event.album()), event.content)
            }

            is SoftwareApplicationEvent -> {
                tiers(event.name(), event.summary(), event.content, website = join(event.url(), event.repository()))
            }

            is PodcastEpisodeEvent -> {
                tiers(event.title(), event.description(), event.content)
            }

            is PodcastMetadataEvent -> {
                tiers(event.title(), event.description(), null, website = join(event.websites()))
            }

            is GroupMetadataEvent -> {
                tiers(event.name(), event.about(), null)
            }

            is InterestSetEvent -> {
                tiers(event.title(), event.description(), null)
            }

            is FollowListEvent -> {
                tiers(event.title(), event.description(), null)
            }

            is MediaStarterPackEvent -> {
                tiers(event.title(), event.description(), null)
            }

            is PictureCurationSetEvent -> {
                tiers(event.title(), event.description(), null)
            }

            is ArticleCurationSetEvent -> {
                tiers(event.title(), event.description(), null)
            }

            is VideoCurationSetEvent -> {
                tiers(event.title(), event.description(), null)
            }

            is ReleaseArtifactSetEvent -> {
                tiers(event.title(), event.description(), null)
            }

            is AppCurationSetEvent -> {
                tiers(event.title(), event.description(), null)
            }

            is RelaySetEvent -> {
                tiers(event.title(), event.description(), null)
            }

            // A web bookmark IS its URL — route it to the affiliation website
            // column so the bookmark is findable by its domain.
            is WebBookmarkEvent -> {
                tiers(event.title(), event.description(), null, website = event.url())
            }

            is NamedSiteEvent -> {
                tiers(event.title(), event.description(), null)
            }

            is RootSiteEvent -> {
                tiers(event.title(), event.description(), null)
            }

            is RootNappletEvent -> {
                tiers(event.title(), event.description(), null)
            }

            is NappletSnapshotEvent -> {
                tiers(event.title(), event.description(), null)
            }

            is NamedNappletEvent -> {
                tiers(event.title(), event.description(), null)
            }

            is FeedDefinitionEvent -> {
                tiers(event.title(), null, null)
            }

            is LabeledBookmarkListEvent -> {
                tiers(event.titleOrName(), event.description(), null)
            }

            is PeopleListEvent -> {
                tiers(event.titleOrName(), event.description(), null)
            }

            is BookmarkListEvent -> {
                tiers(event.title(), null, null)
            }

            is OldBookmarkListEvent -> {
                tiers(event.title(), null, null)
            }

            is GoalEvent -> {
                tiers(null, event.summary(), event.content)
            }

            is HighlightEvent -> {
                tiers(null, join(event.comment(), event.context()), event.content)
            }

            is FileHeaderEvent -> {
                tiers(null, event.summary(), event.content)
            }

            is AudioTrackEvent -> {
                tiers(event.subject(), null, null)
            }

            // Buzz agent/workspace kinds carry their metadata as JSON in `content`;
            // split each decoded object the way its indexableContent() concatenates it.
            is AgentProfileEvent -> {
                event.profileOrNull()?.let { tiers(join(it.name, it.displayName), null, null) } ?: SearchFields.NONE
            }

            is PersonaEvent -> {
                event.personaOrNull()?.let { tiers(it.displayName, null, it.systemPrompt) } ?: SearchFields.NONE
            }

            is ManagedAgentEvent -> {
                event.agentOrNull()?.let { tiers(it.name, null, it.systemPrompt) } ?: SearchFields.NONE
            }

            is TeamEvent -> {
                event.teamOrNull()?.let { tiers(it.name, it.description, it.instructions) } ?: SearchFields.NONE
            }

            is WorkflowDefEvent -> {
                tiers(event.name(), null, event.content)
            }

            // kind 31990 — the app handler's metadata IS a UserMetadata clone,
            // so route it through the kind-0 profile columns: an app's
            // @-handle and site get the same treatment a person's do.
            is AppDefinitionEvent -> {
                val md = event.appMetaData()
                if (md == null) {
                    SearchFields.NONE
                } else {
                    SearchFields(
                        // Per NIP-24 the deprecated `username` folds into `name`.
                        name = clean(md.name ?: md.username),
                        displayName = clean(md.displayName),
                        about = clean(md.about),
                        nip05 = clean(md.nip05),
                        lud16 = clean(md.lud16),
                        website = clean(md.website),
                    )
                }
            }

            // kind 1 LAST among the explicit branches: several kinds extend the
            // text-note base, and their own branches above must win.
            is TextNoteEvent -> {
                tiers(event.subject(), null, event.content)
            }

            // Everything else Quartz can search, current or future: the whole
            // indexableContent lands in the tertiary tier.
            is SearchableEvent -> {
                tiers(null, null, event.indexableContent())
            }

            else -> {
                SearchFields.NONE
            }
        }

    private fun tiers(
        primary: String?,
        secondary: String?,
        text: String?,
        website: String? = null,
    ) = SearchFields(primary = clean(primary), secondary = clean(secondary), text = clean(text), website = clean(website))

    /**
     * Trim, drop empties, and strip what the engine will not store — at the
     * single funnel every derived string passes through: `content` is JSON, so
     * an illegal code point is invisible until parsed out, and one reaching the
     * feed costs the entire batch. See [VespaText.sanitize].
     */
    private fun clean(s: String?): String? = s?.let { VespaText.sanitize(it) }?.trim()?.ifEmpty { null }

    private fun join(vararg parts: String?): String? =
        parts
            .mapNotNull { clean(it) }
            .joinToString("\n")
            .ifEmpty { null }

    private fun join(parts: List<String>): String? = clean(parts.joinToString(" "))

    private fun hashtags(event: Event): String? = join(event.tags.hashtags())

    /** First value of every tag named [name] (e.g. "location", "file"), non-empty only. */
    private fun tagValues(
        event: Event,
        name: String,
    ): List<String> = event.tags.mapNotNull { tag -> tag.getOrNull(1)?.takeIf { tag.getOrNull(0) == name && it.isNotEmpty() } }
}
